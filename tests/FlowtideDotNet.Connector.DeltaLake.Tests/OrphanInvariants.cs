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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// What an orphan cell must leave: retained references present, deletions targeted, directories kept, the cap, one root listing per attempt and nothing held.
    /// </summary>
    internal sealed class OrphanInvariants
    {
        // No restart form: a run is volatile and a restart replans; an unended run's targets file and lease are its cell's assertion
        private const string Log = "_delta_log/";
        private const string Z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
        private static readonly Regex s_commit = new Regex(@"^(\d{20})\.json$");
        private static readonly Regex s_checkpoint = new Regex(@"^\d{20}\.checkpoint\.");
        private static readonly Regex s_stage = new Regex(@"^\.\d{20}\.json\..+\.tmp$");

        private readonly OrphanTable _table;
        private readonly LoopbackObjectStore? _server;
        // Names below the table at capture, exact case
        private readonly HashSet<string> _files;
        // Local folders below the table at capture
        private readonly HashSet<string> _directories;
        // References of the retained versions and of the stages listed at capture
        private readonly HashSet<string> _retained;
        // Request lines before capture, and the last of them
        private readonly int _requestsBefore;
        private readonly string? _lastRequest;
        // Each recorded run's targets by name below the table
        private readonly Dictionary<OrphanRun, IReadOnlyList<string>> _targets = new Dictionary<OrphanRun, IReadOnlyList<string>>();

        private OrphanInvariants(OrphanTable table, LoopbackObjectStore? server, HashSet<string> files, HashSet<string> directories, HashSet<string> retained)
        {
            _table = table;
            _server = server;
            _files = files;
            _directories = directories;
            _retained = retained;
            var requests = server?.Requests.ToArray() ?? Array.Empty<string>();
            _requestsBefore = requests.Length;
            _lastRequest = requests.LastOrDefault();
        }

        /// <summary>
        /// Before a cell's first sweep: the names and folders present and the references of every listed commit and stage, the stages read now.
        /// </summary>
        public static async Task<OrphanInvariants> CaptureAsync(OrphanTable table, LoopbackObjectStore? server)
        {
            Assert.True((server == null) == (table.Folder != null), "The kit takes a local table, or a loopback table with its server");
            var (files, directories) = Present(table, server);
            var retained = await RetainedAsync(table, server, files);
            return new OrphanInvariants(table, server, files, directories, retained);
        }

        /// <summary>
        /// Keeps a run's targets before its delete turns end it and remove its targets file; returns them by name below the table.
        /// </summary>
        public IReadOnlyList<string> Record(OrphanRun run)
        {
            if (!_targets.TryGetValue(run, out var targets))
            {
                targets = OrphanSweepTests.TargetsOf(run, _table);
                _targets[run] = targets;
            }
            return targets;
        }

        /// <summary>
        /// After the cell, with every context settled; removedByTheCell names each file the cell itself removed, log files included.
        /// </summary>
        public async Task AssertAsync(IReadOnlyCollection<OrphanRun> runs, int attempts, int maxDeletions = OrphanSweep.DefaultMaxDeletions, IEnumerable<string>? removedByTheCell = null,
            params CleanupContext[] contexts)
        {
            var (files, directories) = Present(_table, _server);
            var removed = new HashSet<string>(removedByTheCell ?? Array.Empty<string>(), StringComparer.Ordinal);
            var targets = new HashSet<string>(runs.SelectMany(TargetsOf), StringComparer.Ordinal);
            var deletes = Deletes();

            // Retained references, the end reading only the stages still present
            var retained = new HashSet<string>(_retained, StringComparer.Ordinal);
            retained.UnionWith(await RetainedAsync(_table, _server, files));
            foreach (var name in _files.Where(x => retained.Contains(Key(x))))
            {
                Assert.True(files.Contains(name), $"A retained reference was deleted: {name}");
            }

            // No directory removed
            foreach (var directory in _directories)
            {
                Assert.True(directories.Contains(directory), $"A directory was removed: {directory}");
            }
            if (_server?.Flavor == StoreFlavor.AzureHns)
            {
                foreach (var delete in deletes)
                {
                    Assert.False(_files.Any(x => x.StartsWith(delete + "/", StringComparison.Ordinal)), $"A DELETE named a directory: {delete}");
                }
            }

            // Deleted names are targets
            foreach (var name in _files.Where(x => !files.Contains(x) && !removed.Contains(x)))
            {
                Assert.True(targets.Contains(name), $"Deleted without a target: {name}");
            }
            foreach (var delete in deletes)
            {
                Assert.True(targets.Contains(delete) || removed.Contains(delete), $"A DELETE named no target: {delete}");
            }

            foreach (var run in runs)
            {
                Assert.True(run.Targets.Count <= maxDeletions, $"A run of {run.Targets.Count} targets is over the {maxDeletions} deletion cap");
            }
            Assert.True(_table.Owned.Recursive.Count == attempts, $"{_table.Owned.Recursive.Count} root listings for {attempts} attempts");

            // Nothing held
            foreach (var context in contexts)
            {
                Assert.True(context.Memory.Managed == 0, $"A context holds {context.Memory.Managed} managed bytes");
                Assert.True(context.Scratch.Files.Count == 0, "A context holds scratch files: " + string.Join(", ", context.Scratch.Files));
            }
            foreach (var run in runs.Where(x => x.Ended))
            {
                Assert.False(File.Exists(run.Targets.Path), $"An ended run left its targets file: {run.Targets.Path}");
            }
        }

        private IReadOnlyList<string> TargetsOf(OrphanRun run)
        {
            if (_targets.TryGetValue(run, out var targets))
            {
                return targets;
            }
            Assert.False(run.Ended, "An ended run's targets file is gone: record its targets before its delete turns");
            return Record(run);
        }

        // Local names compare upper-cased, as the oracle and local identity fold them
        private string Key(string name) => _server == null ? RetainedReferenceOracle.Upper(name) : name;

        // Names below the table each DELETE since capture named, decoded
        private List<string> Deletes()
        {
            var deletes = new List<string>();
            if (_server == null)
            {
                return deletes;
            }
            var requests = _server.Requests.ToArray();
            // A cleared queue holds only lines after capture
            var start = requests.Length >= _requestsBefore && (_requestsBefore == 0 || ReferenceEquals(requests[_requestsBefore - 1], _lastRequest)) ? _requestsBefore : 0;
            var account = _server.EndpointPath.Length > 0 ? "/" + _server.EndpointPath.TrimEnd('/') : "";
            foreach (var line in requests.Skip(start).Where(x => x.StartsWith("DELETE ", StringComparison.Ordinal)))
            {
                var path = Uri.UnescapeDataString(line.Split(' ')[1].Split('?')[0])[account.Length..];
                Assert.True(path.StartsWith(_table.Prefix, StringComparison.Ordinal), $"A DELETE outside the table: {line}");
                deletes.Add(path[_table.Prefix.Length..]);
            }
            return deletes;
        }

        // Files and local folders below the table, never through a link
        private static (HashSet<string> Files, HashSet<string> Directories) Present(OrphanTable table, LoopbackObjectStore? server)
        {
            var files = new HashSet<string>(StringComparer.Ordinal);
            var directories = new HashSet<string>(StringComparer.Ordinal);
            if (server != null)
            {
                var container = "/" + LoopbackObjectStore.Container + "/";
                Assert.StartsWith(container, table.Prefix, StringComparison.Ordinal);
                var prefix = table.Prefix[container.Length..];
                files.UnionWith(server.Names.Where(x => x.StartsWith(prefix, StringComparison.Ordinal)).Select(x => x[prefix.Length..]));
                return (files, directories);
            }
            var options = new EnumerationOptions() { AttributesToSkip = 0, IgnoreInaccessible = false, RecurseSubdirectories = false };
            var pending = new Stack<DirectoryInfo>();
            pending.Push(new DirectoryInfo(table.Folder!));
            while (pending.TryPop(out var folder))
            {
                foreach (var entry in folder.EnumerateFileSystemInfos("*", options))
                {
                    var name = Path.GetRelativePath(table.Folder!, entry.FullName).Replace('\\', '/');
                    if (entry is not DirectoryInfo child)
                    {
                        files.Add(name);
                        continue;
                    }
                    directories.Add(name);
                    if (!child.Attributes.HasFlag(FileAttributes.ReparsePoint))
                    {
                        pending.Push(child);
                    }
                }
            }
            return (files, directories);
        }

        // live(O) and the references of commits O to head and of the listed stages; O is the oldest listed commit
        private static async Task<HashSet<string>> RetainedAsync(OrphanTable table, LoopbackObjectStore? server, HashSet<string> files)
        {
            var log = files.Where(x => x.StartsWith(Log, StringComparison.Ordinal) && x.IndexOf('/', Log.Length) < 0).Select(x => x[Log.Length..]).ToList();
            var commits = log.Select(x => s_commit.Match(x)).Where(x => x.Success).Select(x => long.Parse(x.Groups[1].Value)).Order().ToList();
            var stages = log.Where(x => s_stage.IsMatch(x)).ToList();
            Assert.True(commits.Count > 0, "The kit needs a listed commit");
            var o = commits[0];
            if (server == null)
            {
                var rootAtO = log.Contains($"{o:D20}.checkpoint.parquet");
                Assert.True(o == 0 || rootAtO, $"The kit needs a classic checkpoint at the oldest listed commit {o}");
                var oracle = await RetainedReferenceOracle.ComputeAsync(table.Folder!, o, rootAtO, false, o, commits[^1], stages);
                return new HashSet<string>(oracle.Candidates.Concat(oracle.Retained), StringComparer.Ordinal);
            }
            Assert.False(log.Any(x => s_checkpoint.IsMatch(x)), "Unsupported: the cloud oracle reads commit-only tables");
            Assert.True(o == 0, $"Unsupported: the cloud oracle needs commit 0, the oldest listed is {o}");
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var name in commits.Select(x => $"{x:D20}.json").Concat(stages))
            {
                var bytes = await DeltaTestKit.ReadBytes(table.Raw, table.Prefix + Log + name);
                foreach (var line in Encoding.UTF8.GetString(bytes).Split('\n').Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    AddReferences(names, line);
                }
            }
            return names;
        }

        // The add, remove and cdc paths of one line and their vectors' files, exact case
        private static void AddReferences(HashSet<string> names, string line)
        {
            using var document = JsonDocument.Parse(line);
            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (property.Name is not ("add" or "remove" or "cdc") || property.Value.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }
                var path = property.Value.GetProperty("path").GetString()!;
                Assert.False(path.StartsWith('/') || path.Contains("://", StringComparison.Ordinal), $"Unsupported: an absolute reference {path}");
                names.Add(Uri.UnescapeDataString(path));
                if (property.Name == "cdc" || !property.Value.TryGetProperty("deletionVector", out var vector) || vector.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }
                var type = vector.GetProperty("storageType").GetString();
                Assert.True(type is "u" or "i", $"Unsupported: a deletion vector of storage type {type}");
                if (type == "u")
                {
                    var value = vector.GetProperty("pathOrInlineDv").GetString()!;
                    var file = $"deletion_vector_{DecodeZ85(value[^20..]):D}.bin";
                    names.Add(value.Length == 20 ? file : value[..^20] + "/" + file);
                }
            }
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
    }
}
