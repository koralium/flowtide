// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow;
using Apache.Arrow.Types;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // An advance's retired files from every reference of its deleted and retained versions; reads a local table with its own JSON, Parquet, URI and Z85 decoding
    internal static class RetainedReferenceOracle
    {
        private const string Z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

        // One physical file a reference names: its table-relative name, and whether the reference spelled it absolutely
        private readonly record struct Named(string Name, bool Absolute);

        // An add or remove: its data file, its vector's file and the vector's offset in it
        private sealed record FileAction(string Path, string? StorageType, string? Vector, long? Offset);

        internal sealed record Result(SortedSet<string> Candidates, SortedSet<string> Retained)
        {
            // Upper-cased table-relative names, as local disk compares them
            public SortedSet<string> Retired => new SortedSet<string>(Candidates.Except(Retained), StringComparer.Ordinal);
        }

        /// <summary>
        /// Candidates are the files every version in [O, C) references, live or by its commit, plus tombstones(O) when asked; retained are the files every version in [C, head] and every stage references.
        /// </summary>
        public static async Task<Result> ComputeAsync(string nativeTable, long o, bool rootAtO, bool tombstonesAtO, long c, long head, IEnumerable<string> stageNames)
        {
            var root = nativeTable.Replace('\\', '/').TrimEnd('/');
            var log = Path.Combine(nativeTable, "_delta_log");
            var candidates = new SortedSet<string>(StringComparer.Ordinal);
            var retained = new SortedSet<string>(StringComparer.Ordinal);
            // Live files by their spec key, path plus vector
            var live = new Dictionary<string, FileAction>(StringComparer.Ordinal);

            var start = 0L;
            if (rootAtO)
            {
                var (adds, removes) = await ReadRootAsync(Path.Combine(log, $"{o:D20}.checkpoint.parquet"));
                foreach (var add in adds)
                {
                    live[Key(add)] = add;
                }
                if (tombstonesAtO)
                {
                    foreach (var remove in removes)
                    {
                        AddCandidates(candidates, Names(remove, root));
                    }
                }
                // live(O) from the root, and the root's own commit when it is still there
                foreach (var file in live.Values)
                {
                    AddCandidates(candidates, Names(file, root));
                }
                var own = Path.Combine(log, $"{o:D20}.json");
                if (File.Exists(own))
                {
                    foreach (var (_, action) in Actions(File.ReadAllLines(own)))
                    {
                        AddCandidates(candidates, Names(action, root));
                    }
                }
                start = o + 1;
            }
            for (var version = start; version <= c; version++)
            {
                foreach (var (kind, action) in Actions(File.ReadAllLines(Path.Combine(log, $"{version:D20}.json"))))
                {
                    var names = Names(action, root);
                    if (version >= o && version < c)
                    {
                        AddCandidates(candidates, names);
                    }
                    else if (version == c)
                    {
                        AddRetained(retained, names);
                    }
                    if (kind == "add")
                    {
                        live[Key(action)] = action;
                    }
                    else if (kind == "remove")
                    {
                        live.Remove(Key(action));
                    }
                }
                if (version == o)
                {
                    // live(O) from the replay
                    foreach (var file in live.Values)
                    {
                        AddCandidates(candidates, Names(file, root));
                    }
                }
            }
            // live(C) from the replay, never from checkpoint C
            foreach (var file in live.Values)
            {
                AddRetained(retained, Names(file, root));
            }
            for (var version = c + 1; version <= head; version++)
            {
                foreach (var (_, action) in Actions(File.ReadAllLines(Path.Combine(log, $"{version:D20}.json"))))
                {
                    AddRetained(retained, Names(action, root));
                }
            }
            foreach (var stage in stageNames)
            {
                foreach (var (_, action) in Actions(File.ReadAllLines(Path.Combine(log, stage))))
                {
                    AddRetained(retained, Names(action, root));
                }
            }
            return new Result(candidates, retained);
        }

        /// <summary>
        /// Writes an empty file for every name inside the table, outside its log, that a commit, a classic checkpoint or a stage references and no entry holds yet.
        /// </summary>
        public static async Task CreateReferencedFilesAsync(string nativeTable)
        {
            var root = nativeTable.Replace('\\', '/').TrimEnd('/');
            var folder = Path.GetFullPath(nativeTable).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
            var log = folder + "_delta_log" + Path.DirectorySeparatorChar;
            var actions = new List<FileAction>();
            // A torn checkpoint, a line or a reference the planner cannot read names nothing here
            foreach (var file in Directory.GetFiles(Path.Combine(nativeTable, "_delta_log")))
            {
                var name = Path.GetFileName(file);
                if (name.EndsWith(".checkpoint.parquet", StringComparison.Ordinal))
                {
                    try
                    {
                        var (adds, removes) = await ReadRootAsync(file);
                        actions.AddRange(adds.Concat(removes));
                    }
                    catch (Exception)
                    {
                    }
                }
                else if (name.EndsWith(".json", StringComparison.Ordinal) || name.EndsWith(".tmp", StringComparison.Ordinal))
                {
                    foreach (var line in File.ReadAllLines(file))
                    {
                        try
                        {
                            actions.AddRange(Actions(new[] { line }).Select(x => x.Action));
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
            }
            foreach (var action in actions)
            {
                try
                {
                    foreach (var named in Names(action, root).Where(x => !x.Name.StartsWith("<outside>", StringComparison.Ordinal)).ToList())
                    {
                        var path = Path.GetFullPath(Path.Combine(nativeTable, named.Name.Replace('/', Path.DirectorySeparatorChar)));
                        // Never outside the table or inside its log
                        if (!path.StartsWith(folder, StringComparison.Ordinal) || path.StartsWith(log, StringComparison.OrdinalIgnoreCase) || File.Exists(path) || Directory.Exists(path))
                        {
                            continue;
                        }
                        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                        File.WriteAllBytes(path, System.Array.Empty<byte>());
                    }
                }
                catch (Exception)
                {
                }
            }
        }

        // An absolute reference is never a candidate
        private static void AddCandidates(SortedSet<string> candidates, IEnumerable<Named> names)
        {
            foreach (var name in names)
            {
                if (!name.Absolute)
                {
                    candidates.Add(Upper(name.Name));
                }
            }
        }

        private static void AddRetained(SortedSet<string> retained, IEnumerable<Named> names)
        {
            foreach (var name in names)
            {
                retained.Add(Upper(name.Name));
            }
        }

        // The spec's file key: the path plus the vector's unique id, its offset included
        private static string Key(FileAction action) => $"{action.Path}|{action.StorageType}{action.Vector}{(action.Offset is { } offset ? "@" + offset : "")}";

        private static IEnumerable<(string Kind, FileAction Action)> Actions(IEnumerable<string> lines)
        {
            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }
                using var document = JsonDocument.Parse(line);
                foreach (var property in document.RootElement.EnumerateObject())
                {
                    if (property.Name is not ("add" or "remove" or "cdc") || property.Value.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }
                    var body = property.Value;
                    string? type = null;
                    string? vector = null;
                    long? offset = null;
                    if (property.Name != "cdc" && body.TryGetProperty("deletionVector", out var dv) && dv.ValueKind == JsonValueKind.Object)
                    {
                        type = dv.GetProperty("storageType").GetString();
                        vector = dv.GetProperty("pathOrInlineDv").GetString();
                        if (dv.TryGetProperty("offset", out var at) && at.ValueKind == JsonValueKind.Number)
                        {
                            offset = at.GetInt64();
                        }
                    }
                    yield return (property.Name, new FileAction(body.GetProperty("path").GetString()!, type, vector, offset));
                }
            }
        }

        // The data file, then the vector's file: a uuid vector's Z85 id under its prefix, an absolute vector's URI, none inline
        private static IEnumerable<Named> Names(FileAction action, string root)
        {
            yield return FromUri(action.Path, root);
            switch (action.StorageType)
            {
                case "u":
                    var value = action.Vector!;
                    var prefix = value.Substring(0, value.Length - 20);
                    var name = $"deletion_vector_{DecodeZ85(value.Substring(value.Length - 20)):D}.bin";
                    yield return new Named(prefix.Length == 0 ? name : prefix + "/" + name, false);
                    break;
                case "p":
                    yield return FromUri(action.Vector!, root);
                    break;
            }
        }

        // A relative URI decodes once; an absolute one names a file inside the table only under its root
        private static Named FromUri(string value, string root)
        {
            string? absolute = null;
            if (value.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
            {
                absolute = value.Substring(5);
                if (absolute.StartsWith("//", StringComparison.Ordinal))
                {
                    absolute = absolute.Substring(2);
                    if (absolute.StartsWith("localhost", StringComparison.OrdinalIgnoreCase))
                    {
                        absolute = absolute.Substring(9);
                    }
                }
            }
            else if (value.StartsWith('/') || (value.Length > 2 && value[1] == ':'))
            {
                absolute = value;
            }
            if (absolute == null)
            {
                return new Named(PercentDecode(value), false);
            }
            var decoded = PercentDecode(absolute);
            // A Windows drive path has a slash before its drive letter in a URI
            if (decoded.Length > 3 && decoded[0] == '/' && decoded[2] == ':')
            {
                decoded = decoded.Substring(1);
            }
            var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
            if (!decoded.StartsWith(root + "/", comparison))
            {
                return new Named("<outside>" + decoded, true);
            }
            return new Named(decoded.Substring(root.Length + 1), true);
        }

        private static string PercentDecode(string value)
        {
            var bytes = new List<byte>();
            for (int i = 0; i < value.Length; i++)
            {
                if (value[i] == '%')
                {
                    bytes.Add(Convert.ToByte(value.Substring(i + 1, 2), 16));
                    i += 2;
                }
                else
                {
                    bytes.AddRange(Encoding.UTF8.GetBytes(value[i].ToString()));
                }
            }
            return Encoding.UTF8.GetString(bytes.ToArray());
        }

        // Four big-endian 32-bit groups of five base-85 digits each make the uuid's bytes
        private static Guid DecodeZ85(string encoded)
        {
            var bytes = new byte[16];
            for (int group = 0; group < 4; group++)
            {
                uint value = 0;
                for (int i = 0; i < 5; i++)
                {
                    value = checked(value * 85 + (uint)Z85Alphabet.IndexOf(encoded[group * 5 + i]));
                }
                BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(group * 4), value);
            }
            return new Guid(bytes, bigEndian: true);
        }

        internal static string Upper(string name)
        {
            var builder = new StringBuilder(name.Length);
            foreach (var c in name)
            {
                builder.Append(c is >= 'a' and <= 'z' ? (char)(c - 32) : c);
            }
            return builder.ToString();
        }

        // A root's adds and removes, read through Arrow
        private static async Task<(List<FileAction> Adds, List<FileAction> Removes)> ReadRootAsync(string path)
        {
            var adds = new List<FileAction>();
            var removes = new List<FileAction>();
            using var reader = new ParquetSharp.Arrow.FileReader(path);
            using var batches = reader.GetRecordBatchReader();
            RecordBatch batch;
            while ((batch = await batches.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    Collect((StructArray)batch.Column("add"), adds);
                    Collect((StructArray)batch.Column("remove"), removes);
                }
            }
            return (adds, removes);
        }

        private static void Collect(StructArray actions, List<FileAction> into)
        {
            var type = (StructType)actions.Data.DataType;
            var paths = (StringArray)actions.Fields[type.GetFieldIndex("path")];
            var vectorIndex = type.GetFieldIndex("deletionVector");
            var vectors = vectorIndex < 0 ? null : (StructArray)actions.Fields[vectorIndex];
            var vectorType = (StructType?)vectors?.Data.DataType;
            var storageTypes = vectors == null ? null : (StringArray)vectors.Fields[vectorType!.GetFieldIndex("storageType")];
            var values = vectors == null ? null : (StringArray)vectors.Fields[vectorType!.GetFieldIndex("pathOrInlineDv")];
            var offsetIndex = vectorType?.GetFieldIndex("offset") ?? -1;
            var offsets = offsetIndex < 0 ? null : vectors!.Fields[offsetIndex];
            for (int i = 0; i < actions.Length; i++)
            {
                if (actions.IsNull(i))
                {
                    continue;
                }
                var hasVector = vectors != null && !vectors.IsNull(i);
                into.Add(new FileAction(paths.GetString(i), hasVector ? storageTypes!.GetString(i) : null, hasVector ? values!.GetString(i) : null, hasVector ? OffsetAt(offsets, i) : null));
            }
        }

        // An int offset as the spec writes it, or a long one
        private static long? OffsetAt(IArrowArray? offsets, int index)
        {
            return offsets switch
            {
                null => null,
                Int32Array narrow => narrow.GetValue(index),
                Int64Array wide => wide.GetValue(index),
                _ => throw new InvalidDataException($"A deletion vector offset of type {offsets.Data.DataType.Name}")
            };
        }
    }
}
