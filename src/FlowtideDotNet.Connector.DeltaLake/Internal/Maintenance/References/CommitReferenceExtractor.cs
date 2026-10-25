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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References
{
    /// <summary>
    /// Where a physical path came from.
    /// </summary>
    internal enum ReferenceSource : byte
    {
        Add = 0,
        Remove = 1,
        Cdc = 2,
        AddVector = 3,
        RemoveVector = 4,
        // A listed or owned name
        Name = 5,
        // A line or record as a whole
        Action = 6
    }

    /// <summary>
    /// One physical path an action references, as the action holds it; a rejection means its file cannot be named.
    /// </summary>
    internal readonly struct PathReference
    {
        public PathReference(ReferenceSource source, string? value, bool isUri, bool absoluteOnly = false, PathRejection rejection = PathRejection.None)
        {
            Source = source;
            Value = value;
            IsUri = isUri;
            AbsoluteOnly = absoluteOnly;
            Rejection = rejection;
        }

        public ReferenceSource Source { get; }

        // JSON escapes resolved, not yet URI-decoded
        public string? Value { get; }

        // A log URI, decoded exactly once; a uuid vector's path is a plain name
        public bool IsUri { get; }

        // An absolute vector path, which never names a relative file
        public bool AbsoluteOnly { get; }

        public PathRejection Rejection { get; }

        public static PathReference Rejected(ReferenceSource source, PathRejection rejection) => new PathReference(source, null, false, false, rejection);
    }

    internal delegate void ReferenceVisitor(in PathReference reference);

    /// <summary>
    /// Collects the physical paths of commit lines, checkpoint records and names into a path set; protection declines at its first unusable path, an unusable candidate is kept and counted.
    /// </summary>
    internal sealed class CommitReferenceExtractor
    {
        // A uuid vector ends in a 20 character Z85 id
        private const int UuidLength = 20;
        private const string Z85Characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

        private static readonly UTF8Encoding s_strict = new UTF8Encoding(false, true);

        private readonly PathCanonicalizer _identity;
        private readonly PathSet _set;
        private readonly ReferenceVisitor _visit;
        private readonly long[] _kept = new long[(int)PathRejection.Unresolved + 1];
        // The paths of the line being read, held until it parses
        private readonly List<PathReference> _pending = new List<PathReference>();

        public CommitReferenceExtractor(PathCanonicalizer identity, PathSet set, PathRole role)
        {
            // Only the role the set was made for
            if (set.Role != role)
            {
                throw new ArgumentException($"A {role} extractor cannot fill a {set.Role} set", nameof(set));
            }
            // Only keys of this canonicalizer's domain
            if (!set.TryBind(identity.Domain))
            {
                throw new ArgumentException($"A {identity.Identity} extractor cannot fill a set of another key domain", nameof(set));
            }
            _identity = identity;
            _set = set;
            Role = role;
            _visit = Visit;
        }

        public PathRole Role { get; }

        // Keys added to the set
        public long Added { get; private set; }

        // The first unusable protection path, None while protection holds
        public PathRejection Declined { get; private set; }

        public ReferenceSource DeclinedSource { get; private set; }

        // Candidates kept, by rejection
        public long Kept
        {
            get
            {
                long total = 0;
                foreach (var count in _kept)
                {
                    total += count;
                }
                return total;
            }
        }

        public long KeptFor(PathRejection rejection) => _kept[(int)rejection];

        // A finite reason for logs, never a path
        public string DeclineDetail => $"{Declined} path in {DeclinedSource}";

        /// <summary>
        /// Adds every path a commit line references; false once protection declined.
        /// </summary>
        public bool AddLine(ReadOnlySpan<byte> line)
        {
            if (Declined != PathRejection.None)
            {
                return false;
            }
            return Complete(ReadLine(line, _visit));
        }

        /// <summary>
        /// Adds the paths of one checkpoint record; false once protection declined.
        /// </summary>
        public bool AddRecord(ReadOnlySpan<byte> record)
        {
            if (Declined != PathRejection.None)
            {
                return false;
            }
            return Complete(ReadRecord(record, _visit));
        }

        // A line or record that does not parse offers none of its paths
        private bool Complete(PathRejection problem)
        {
            try
            {
                if (problem != PathRejection.None)
                {
                    Reject(problem, ReferenceSource.Action);
                    return Declined == PathRejection.None;
                }
                foreach (var reference in _pending)
                {
                    if (Declined != PathRejection.None)
                    {
                        break;
                    }
                    Accept(_identity.Reference(reference, Role), reference.Source);
                }
                return Declined == PathRejection.None;
            }
            finally
            {
                _pending.Clear();
            }
        }

        /// <summary>
        /// Adds a physical table-relative name, never decoded; false once protection declined.
        /// </summary>
        public bool AddName(string name)
        {
            if (Declined != PathRejection.None)
            {
                return false;
            }
            Accept(_identity.Name(name, Role), ReferenceSource.Name);
            return Declined == PathRejection.None;
        }

        private void Visit(in PathReference reference)
        {
            _pending.Add(reference);
        }

        private void Accept(PathKey key, ReferenceSource source)
        {
            if (!key.Accepted)
            {
                Reject(key.Rejection, source);
                return;
            }
            _set.Add(key.Key!, key.DeletionPath);
            Added++;
        }

        private void Reject(PathRejection rejection, ReferenceSource source)
        {
            if (Role == PathRole.Candidate)
            {
                _kept[(int)rejection]++;
                return;
            }
            if (Declined == PathRejection.None)
            {
                Declined = rejection;
                DeclinedSource = source;
            }
        }

        /// <summary>
        /// Visits every file path of an add, remove or cdc action on a commit line, each repetition included; Malformed when the line is not one JSON object.
        /// </summary>
        public static PathRejection ReadLine(ReadOnlySpan<byte> line, ReferenceVisitor visitor)
        {
            if (BoundedLineReader.IsBlank(line))
            {
                return PathRejection.None;
            }
            try
            {
                var reader = new Utf8JsonReader(line);
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                {
                    return PathRejection.Malformed;
                }
                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndObject)
                    {
                        // A second value after the object is malformed
                        return reader.Read() ? PathRejection.Malformed : PathRejection.None;
                    }
                    ReferenceSource? source = reader.ValueTextEquals("add"u8) ? ReferenceSource.Add
                        : reader.ValueTextEquals("remove"u8) ? ReferenceSource.Remove
                        : reader.ValueTextEquals("cdc"u8) ? ReferenceSource.Cdc
                        : null;
                    reader.Read();
                    if (source == null || reader.TokenType == JsonTokenType.Null)
                    {
                        reader.Skip();
                        continue;
                    }
                    if (reader.TokenType != JsonTokenType.StartObject)
                    {
                        return PathRejection.Malformed;
                    }
                    var problem = ReadFileAction(ref reader, source.Value, visitor);
                    if (problem != PathRejection.None)
                    {
                        return problem;
                    }
                }
                return PathRejection.Malformed;
            }
            catch (JsonException)
            {
                return PathRejection.Malformed;
            }
        }

        private static PathRejection ReadFileAction(ref Utf8JsonReader reader, ReferenceSource source, ReferenceVisitor visitor)
        {
            var paths = 0;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.ValueTextEquals("path"u8))
                {
                    reader.Read();
                    if (reader.TokenType == JsonTokenType.String)
                    {
                        visitor(TryGetText(ref reader, out var text) ? new PathReference(source, text, isUri: true) : PathReference.Rejected(source, PathRejection.Encoding));
                        paths++;
                    }
                    else if (reader.TokenType != JsonTokenType.Null)
                    {
                        return PathRejection.Malformed;
                    }
                }
                else if (source != ReferenceSource.Cdc && reader.ValueTextEquals("deletionVector"u8))
                {
                    reader.Read();
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        ReadVector(ref reader, source == ReferenceSource.Add ? ReferenceSource.AddVector : ReferenceSource.RemoveVector, visitor);
                    }
                    else if (reader.TokenType != JsonTokenType.Null)
                    {
                        return PathRejection.Malformed;
                    }
                }
                else
                {
                    reader.Read();
                    reader.Skip();
                }
            }
            if (paths == 0)
            {
                visitor(PathReference.Rejected(source, PathRejection.Malformed));
            }
            return PathRejection.None;
        }

        private static void ReadVector(ref Utf8JsonReader reader, ReferenceSource source, ReferenceVisitor visitor)
        {
            string? storageType = null;
            string? value = null;
            var types = 0;
            var values = 0;
            var undecodable = false;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                var isType = reader.ValueTextEquals("storageType"u8);
                var isValue = !isType && reader.ValueTextEquals("pathOrInlineDv"u8);
                reader.Read();
                if (!isType && !isValue)
                {
                    reader.Skip();
                    continue;
                }
                if (reader.TokenType != JsonTokenType.String && reader.TokenType != JsonTokenType.Null)
                {
                    throw new JsonException("A deletion vector field is not a string");
                }
                string? text = null;
                if (reader.TokenType == JsonTokenType.String && !TryGetText(ref reader, out text))
                {
                    undecodable = true;
                }
                if (isType)
                {
                    storageType = text;
                    types++;
                }
                else
                {
                    value = text;
                    values++;
                }
            }
            if (undecodable)
            {
                visitor(PathReference.Rejected(source, PathRejection.Encoding));
                return;
            }
            // A repeated field leaves the vector's file ambiguous
            if (types > 1 || values > 1)
            {
                visitor(PathReference.Rejected(source, PathRejection.InvalidVector));
                return;
            }
            VisitVector(source, storageType, value, visitor);
        }

        /// <summary>
        /// Visits the paths of an add or remove checkpoint record and its vector; other records reference no file.
        /// </summary>
        public static PathRejection ReadRecord(ReadOnlySpan<byte> record, ReferenceVisitor visitor)
        {
            try
            {
                var r = new RecordReader(record);
                var kind = r.ReadKind(out _);
                if (kind != RecordKind.Add && kind != RecordKind.Remove)
                {
                    return PathRejection.None;
                }
                var source = kind == RecordKind.Add ? ReferenceSource.Add : ReferenceSource.Remove;
                if (!r.ReadUtf8(out var path))
                {
                    visitor(PathReference.Rejected(source, PathRejection.Malformed));
                }
                else if (TryText(path, out var text))
                {
                    visitor(new PathReference(source, text, isUri: true));
                }
                else
                {
                    visitor(PathReference.Rejected(source, PathRejection.Encoding));
                }
                if (kind == RecordKind.Add)
                {
                    r.SkipMap();
                    r.SkipLong();
                    r.SkipLong();
                    r.ReadBool();
                    r.SkipString();
                    r.SkipMap();
                }
                else
                {
                    r.SkipLong();
                    r.ReadBool();
                    r.ReadBool();
                    r.SkipMap();
                    r.SkipLong();
                }
                if (!r.ReadStruct())
                {
                    return PathRejection.None;
                }
                var vectorSource = kind == RecordKind.Add ? ReferenceSource.AddVector : ReferenceSource.RemoveVector;
                var hasType = r.ReadUtf8(out var storageType);
                var hasValue = r.ReadUtf8(out var value);
                string? typeText = null;
                string? valueText = null;
                if ((hasType && !TryText(storageType, out typeText)) || (hasValue && !TryText(value, out valueText)))
                {
                    visitor(PathReference.Rejected(vectorSource, PathRejection.Encoding));
                    return PathRejection.None;
                }
                VisitVector(vectorSource, typeText, valueText, visitor);
                return PathRejection.None;
            }
            catch (InvalidDataException)
            {
                return PathRejection.Malformed;
            }
        }

        // An escaped lone surrogate has no text
        private static bool TryGetText(ref Utf8JsonReader reader, out string? text)
        {
            try
            {
                text = reader.GetString();
                return true;
            }
            catch (InvalidOperationException)
            {
                text = null;
                return false;
            }
        }

        private static bool TryText(ReadOnlySpan<byte> utf8, out string? text)
        {
            try
            {
                text = s_strict.GetString(utf8);
                return true;
            }
            catch (ArgumentException)
            {
                text = null;
                return false;
            }
        }

        // Every offset of a container names the same file; inline vectors have none
        private static void VisitVector(ReferenceSource source, string? storageType, string? value, ReferenceVisitor visitor)
        {
            switch (storageType)
            {
                case "i":
                    return;
                case "u":
                    if (value == null || value.Length < UuidLength || !TryDecodeUuid(value.AsSpan(value.Length - UuidLength), out var id))
                    {
                        visitor(PathReference.Rejected(source, PathRejection.InvalidVector));
                        return;
                    }
                    var name = $"deletion_vector_{id:D}.bin";
                    var prefixLength = value.Length - UuidLength;
                    visitor(new PathReference(source, prefixLength == 0 ? name : value.Substring(0, prefixLength) + "/" + name, isUri: false));
                    return;
                case "p":
                    if (value == null)
                    {
                        visitor(PathReference.Rejected(source, PathRejection.InvalidVector));
                        return;
                    }
                    visitor(new PathReference(source, value, isUri: true, absoluteOnly: true));
                    return;
                default:
                    visitor(PathReference.Rejected(source, PathRejection.UnknownVectorType));
                    return;
            }
        }

        // Each group of five characters must encode a 32-bit value
        private static bool TryDecodeUuid(ReadOnlySpan<char> encoded, out Guid id)
        {
            id = default;
            Span<byte> bytes = stackalloc byte[16];
            for (int group = 0; group < UuidLength / 5; group++)
            {
                ulong value = 0;
                for (int i = group * 5; i < group * 5 + 5; i++)
                {
                    var digit = Z85Characters.IndexOf(encoded[i]);
                    if (digit < 0)
                    {
                        return false;
                    }
                    value = value * 85 + (ulong)digit;
                }
                if (value > uint.MaxValue)
                {
                    return false;
                }
                BinaryPrimitives.WriteUInt32BigEndian(bytes.Slice(group * 4), (uint)value);
            }
            // The uuid's big-endian bytes, as Z85.DecodeToGuid reads them
            id = new Guid(bytes, bigEndian: true);
            return true;
        }
    }
}
