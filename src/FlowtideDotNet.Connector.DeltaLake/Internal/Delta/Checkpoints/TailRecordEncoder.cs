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
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Unicode;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// Receives the records of one line; the span is valid until the next line is encoded.
    /// </summary>
    internal delegate void RecordSink(ReadOnlySpan<byte> record);

    /// <summary>
    /// Encodes UTF-8 log lines into checkpoint records in two passes, accepting what the reader's model accepts.
    /// </summary>
    internal sealed class TailRecordEncoder : IDisposable
    {
        public const int DefaultEntryCap = 65_536;

        private enum Field : byte
        {
            String,
            Long,
            NullableLong,
            Int,
            Bool,
            NullableBool,
            Map,
            List,
            Vector,
            Format,
            Lenient,
            Any
        }

        private readonly struct Property
        {
            public Property(string name, Field type)
            {
                Name = System.Text.Encoding.UTF8.GetBytes(name);
                Type = type;
            }

            public byte[] Name { get; }

            public Field Type { get; }
        }

        // Field order is the record order
        private static readonly Property[] s_add =
        {
            new("path", Field.String), new("partitionValues", Field.Map), new("size", Field.Long), new("modificationTime", Field.Long),
            new("dataChange", Field.Bool), new("stats", Field.String), new("tags", Field.Map), new("deletionVector", Field.Vector),
            new("baseRowId", Field.NullableLong), new("defaultRowCommitVersion", Field.NullableLong), new("clusteringProvider", Field.String)
        };

        // stats and tags are validated as the reader reads them, the record never keeps them
        private static readonly Property[] s_remove =
        {
            new("path", Field.String), new("deletionTimestamp", Field.NullableLong), new("dataChange", Field.Bool), new("extendedFileMetadata", Field.Bool),
            new("partitionValues", Field.Map), new("size", Field.NullableLong), new("deletionVector", Field.Vector), new("baseRowId", Field.NullableLong),
            new("defaultRowCommitVersion", Field.NullableLong), new("stats", Field.String), new("tags", Field.Map)
        };
        private const int RemoveWrittenFields = 9;

        private static readonly Property[] s_vector =
        {
            new("storageType", Field.String), new("pathOrInlineDv", Field.String), new("offset", Field.NullableLong), new("sizeInBytes", Field.Int), new("cardinality", Field.Long)
        };

        private static readonly Property[] s_metaData =
        {
            new("id", Field.String), new("name", Field.String), new("description", Field.String), new("format", Field.Format),
            new("schemaString", Field.String), new("partitionColumns", Field.List), new("createdTime", Field.NullableLong), new("configuration", Field.Map)
        };

        private static readonly Property[] s_format = { new("provider", Field.String), new("options", Field.Map) };

        private static readonly Property[] s_protocol =
        {
            new("minReaderVersion", Field.Int), new("minWriterVersion", Field.Int), new("readerFeatures", Field.List), new("writerFeatures", Field.List)
        };

        private static readonly Property[] s_txn = { new("appId", Field.String), new("version", Field.Long), new("lastUpdated", Field.NullableLong) };

        private static readonly Property[] s_domain = { new("domain", Field.String), new("configuration", Field.String), new("removed", Field.Bool) };

        private static readonly Property[] s_cdc =
        {
            new("path", Field.String), new("partitionValues", Field.Map), new("size", Field.Long), new("dataChange", Field.Bool), new("tags", Field.Map)
        };

        private static readonly Property[] s_commitInfo = { new("flowtide.stageId", Field.Lenient) };

        // Top-level actions in the order the reader applies them, then the two that give no record
        private static readonly (byte[] Name, Property[] Fields, RecordKind Kind)[] s_actions =
        {
            ("add"u8.ToArray(), s_add, RecordKind.Add),
            ("remove"u8.ToArray(), s_remove, RecordKind.Remove),
            ("metaData"u8.ToArray(), s_metaData, RecordKind.MetaData),
            ("protocol"u8.ToArray(), s_protocol, RecordKind.Protocol),
            ("txn"u8.ToArray(), s_txn, RecordKind.Txn),
            ("domainMetadata"u8.ToArray(), s_domain, RecordKind.Domain),
            ("cdc"u8.ToArray(), s_cdc, RecordKind.Add),
            ("commitInfo"u8.ToArray(), s_commitInfo, RecordKind.Add)
        };
        private const int RecordActions = 6;
        private const int CommitInfoAction = 7;

        [StructLayout(LayoutKind.Sequential)]
        private struct KeyEntry
        {
            public int Hash;
            public int Start;
            public int Ordinal;
            public int Unused;
        }

        private readonly JobMemory _memory;
        private readonly int _entryCap;
        private byte[]? _record;
        private byte[]? _keyIndex;
        private byte[]? _keyScratch;

        // Per line: value spans of the last occurrence of each action, start and length, -1 when absent or null
        private readonly int[] _actionStart = new int[s_actions.Length];
        private readonly int[] _actionLength = new int[s_actions.Length];
        private long _recordBound;
        private int _largestMap;
        private int _actionEntries;
        private int _longestEscapedKey;
        private int _secondEscapedKey;

        public TailRecordEncoder(JobMemory memory, int entryCap = DefaultEntryCap)
        {
            _memory = memory;
            _entryCap = entryCap;
        }

        /// <summary>
        /// The commitInfo timestamp of the last encoded line, null when it had none.
        /// </summary>
        public long? CommitTimestamp { get; private set; }

        /// <summary>
        /// Encodes one line, calling the sink once per record in the reader's apply order, and returns the record count.
        /// </summary>
        public int Encode(ReadOnlySpan<byte> line, RecordSink sink)
        {
            Validate(line);
            ReserveFor(line);

            var count = 0;
            for (int a = 0; a < RecordActions; a++)
            {
                if (_actionStart[a] < 0)
                {
                    continue;
                }
                var value = line.Slice(_actionStart[a], _actionLength[a]);
                var length = WriteAction(value, a);
                if (length > 0)
                {
                    sink(_record.AsSpan(0, length));
                    count++;
                }
            }
            return count;
        }

        private static CheckpointDeclinedException Invalid(string detail)
        {
            return new CheckpointDeclinedException(MaintenanceReason.InvalidLogLine, DeclineDependency.Tail, detail);
        }

        // First pass: the reader's acceptance rules, spans of the last occurrences, entry counts and the record bound
        private void Validate(ReadOnlySpan<byte> line)
        {
            Array.Fill(_actionStart, -1);
            Array.Fill(_actionLength, 0);
            _recordBound = 0;
            _largestMap = 0;
            _actionEntries = 0;
            _longestEscapedKey = 0;
            _secondEscapedKey = 0;
            CommitTimestamp = null;
            try
            {
                var reader = new Utf8JsonReader(line, new JsonReaderOptions());
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                {
                    throw Invalid("A log line is not a JSON object");
                }
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    CheckString(ref reader);
                    var action = -1;
                    for (int a = 0; a < s_actions.Length; a++)
                    {
                        if (reader.ValueTextEquals(s_actions[a].Name))
                        {
                            action = a;
                            break;
                        }
                    }
                    reader.Read();
                    if (action < 0)
                    {
                        reader.Skip();
                        continue;
                    }
                    // Every occurrence replaces the last, a null one included
                    if (action == CommitInfoAction)
                    {
                        CommitTimestamp = null;
                    }
                    if (reader.TokenType == JsonTokenType.Null)
                    {
                        _actionStart[action] = -1;
                        continue;
                    }
                    if (reader.TokenType != JsonTokenType.StartObject)
                    {
                        throw Invalid($"Action {System.Text.Encoding.UTF8.GetString(s_actions[action].Name)} is not an object");
                    }
                    var start = (int)reader.TokenStartIndex;
                    _actionEntries = 0;
                    if (action == CommitInfoAction)
                    {
                        ValidateCommitInfo(ref reader);
                    }
                    else
                    {
                        ValidateObject(ref reader, s_actions[action].Fields);
                    }
                    _actionStart[action] = start;
                    _actionLength[action] = (int)reader.BytesConsumed - start;
                }
                if (reader.TokenType != JsonTokenType.EndObject)
                {
                    throw Invalid("A log line is not a JSON object");
                }
                // Trailing content fails the reader too
                if (reader.Read())
                {
                    throw Invalid("A log line has content after its object");
                }
            }
            catch (JsonException e)
            {
                throw Invalid(e.Message);
            }
            catch (InvalidOperationException e)
            {
                throw Invalid(e.Message);
            }

            // A record is at most its action's JSON plus its kind byte and absent fields, records reuse one buffer
            for (int a = 0; a < RecordActions; a++)
            {
                if (_actionStart[a] >= 0)
                {
                    _recordBound = Math.Max(_recordBound, 64 + _actionLength[a]);
                }
            }
        }

        private void ValidateCommitInfo(ref Utf8JsonReader reader)
        {
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                CheckString(ref reader);
                var timestamp = reader.ValueTextEquals("timestamp"u8);
                var stageId = reader.ValueTextEquals(s_commitInfo[0].Name);
                reader.Read();
                if (timestamp)
                {
                    CommitTimestamp = reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out var value) ? value : null;
                }
                if (stageId && reader.TokenType == JsonTokenType.String)
                {
                    CheckString(ref reader);
                }
                reader.Skip();
            }
        }

        private void ValidateObject(ref Utf8JsonReader reader, Property[] fields)
        {
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                CheckString(ref reader);
                var field = -1;
                for (int f = 0; f < fields.Length; f++)
                {
                    if (reader.ValueTextEquals(fields[f].Name))
                    {
                        field = f;
                        break;
                    }
                }
                reader.Read();
                if (field < 0)
                {
                    reader.Skip();
                    continue;
                }
                ValidateValue(ref reader, fields[field].Type);
            }
        }

        private void ValidateValue(ref Utf8JsonReader reader, Field type)
        {
            var token = reader.TokenType;
            switch (type)
            {
                case Field.String:
                    if (token != JsonTokenType.String && token != JsonTokenType.Null)
                    {
                        throw Invalid("A string field holds another type");
                    }
                    if (token == JsonTokenType.String)
                    {
                        CheckString(ref reader);
                    }
                    break;
                case Field.Long:
                case Field.Int:
                    if (token != JsonTokenType.Number || !(type == Field.Long ? reader.TryGetInt64(out _) : reader.TryGetInt32(out _)))
                    {
                        throw Invalid("A number field holds another value");
                    }
                    break;
                case Field.NullableLong:
                    if (token != JsonTokenType.Null && (token != JsonTokenType.Number || !reader.TryGetInt64(out _)))
                    {
                        throw Invalid("A number field holds another value");
                    }
                    break;
                case Field.Bool:
                    if (token != JsonTokenType.True && token != JsonTokenType.False)
                    {
                        throw Invalid("A boolean field holds another value");
                    }
                    break;
                case Field.NullableBool:
                    if (token != JsonTokenType.True && token != JsonTokenType.False && token != JsonTokenType.Null)
                    {
                        throw Invalid("A boolean field holds another value");
                    }
                    break;
                case Field.Map:
                case Field.List:
                    ValidateCollection(ref reader, type == Field.Map);
                    break;
                case Field.Vector:
                case Field.Format:
                    if (token == JsonTokenType.Null)
                    {
                        break;
                    }
                    if (token != JsonTokenType.StartObject)
                    {
                        throw Invalid("A struct field holds another type");
                    }
                    ValidateObject(ref reader, type == Field.Vector ? s_vector : s_format);
                    break;
                case Field.Lenient:
                    if (token == JsonTokenType.String)
                    {
                        CheckString(ref reader);
                    }
                    reader.Skip();
                    break;
                case Field.Any:
                    reader.Skip();
                    break;
            }
        }

        private void ValidateCollection(ref Utf8JsonReader reader, bool map)
        {
            if (reader.TokenType == JsonTokenType.Null)
            {
                return;
            }
            if (reader.TokenType != (map ? JsonTokenType.StartObject : JsonTokenType.StartArray))
            {
                throw Invalid(map ? "A map field holds another type" : "A list field holds another type");
            }
            var entries = 0;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject && reader.TokenType != JsonTokenType.EndArray)
            {
                if (map)
                {
                    CheckString(ref reader);
                    if (reader.ValueIsEscaped)
                    {
                        var escaped = reader.ValueSpan.Length;
                        if (escaped > _longestEscapedKey)
                        {
                            _secondEscapedKey = _longestEscapedKey;
                            _longestEscapedKey = escaped;
                        }
                        else if (escaped > _secondEscapedKey)
                        {
                            _secondEscapedKey = escaped;
                        }
                    }
                    reader.Read();
                }
                ValidateValue(ref reader, Field.String);
                entries++;
                // The entry cap holds per action, over all its maps and lists
                if (++_actionEntries > _entryCap)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.EntriesOverCap, DeclineDependency.Tail, $"An action has more than {_entryCap} map entries and list items");
                }
            }
            if (map)
            {
                _largestMap = Math.Max(_largestMap, entries);
            }
        }

        // The reader unescapes this token into a string, which fails on invalid UTF-8 or a lone surrogate escape
        private static void CheckString(ref Utf8JsonReader reader)
        {
            var raw = reader.ValueSpan;
            if (!Utf8.IsValid(raw) || (reader.ValueIsEscaped && !SurrogatesPaired(raw)))
            {
                throw new CheckpointDeclinedException(MaintenanceReason.InvalidUtf8, DeclineDependency.Tail, "A string is not valid UTF-8 or holds a lone surrogate escape");
            }
        }

        // Escapes are well-formed here, the JSON reader checked their syntax
        private static bool SurrogatesPaired(ReadOnlySpan<byte> raw)
        {
            var high = false;
            for (int i = 0; i < raw.Length; i++)
            {
                if (raw[i] != (byte)'\\')
                {
                    if (high)
                    {
                        return false;
                    }
                    continue;
                }
                i++;
                if (raw[i] != (byte)'u')
                {
                    if (high)
                    {
                        return false;
                    }
                    continue;
                }
                var unit = 0;
                for (int h = 1; h <= 4; h++)
                {
                    var c = raw[i + h];
                    unit = unit * 16 + (c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10);
                }
                i += 4;
                if (unit is >= 0xD800 and <= 0xDBFF)
                {
                    if (high)
                    {
                        return false;
                    }
                    high = true;
                }
                else if (unit is >= 0xDC00 and <= 0xDFFF)
                {
                    if (!high)
                    {
                        return false;
                    }
                    high = false;
                }
                else if (high)
                {
                    return false;
                }
            }
            return !high;
        }

        // Grow-only buffers, each growth reserved first
        private void ReserveFor(ReadOnlySpan<byte> line)
        {
            if (_recordBound > line.Length + 1024L)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.RecordOverCap, DeclineDependency.Tail, $"A record bound of {_recordBound} bytes");
            }
            _record = Grow(_record, (int)_recordBound);
            _keyIndex = Grow(_keyIndex, _largestMap * Marshal.SizeOf<KeyEntry>() + (_largestMap + 7) / 8);
            // Two escaped keys are compared at once, unescaping never lengthens a key
            _keyScratch = Grow(_keyScratch, _longestEscapedKey + _secondEscapedKey);
        }

        private byte[]? Grow(byte[]? buffer, int length)
        {
            if (length == 0 || (buffer != null && buffer.Length >= length))
            {
                return buffer;
            }
            // Reserved before the old buffer is released, so a failed growth still owns the old one
            var grown = _memory.Allocate(length);
            if (buffer != null)
            {
                _memory.Free(buffer);
            }
            return grown;
        }

        public void Dispose()
        {
            foreach (var buffer in new[] { _record, _keyIndex, _keyScratch })
            {
                if (buffer != null)
                {
                    _memory.Free(buffer);
                }
            }
            _record = null;
            _keyIndex = null;
            _keyScratch = null;
        }

        // Second pass: one record from the action's last occurrence, 0 when the reader ignores it (txn or domain without an id)
        private int WriteAction(ReadOnlySpan<byte> value, int action)
        {
            var fields = s_actions[action].Fields;
            Span<int> starts = stackalloc int[fields.Length];
            Span<int> lengths = stackalloc int[fields.Length];
            FindFields(value, fields, starts, lengths);

            var kind = s_actions[action].Kind;
            if ((kind == RecordKind.Txn || kind == RecordKind.Domain) && IsNull(value, starts[0], lengths[0]))
            {
                return 0;
            }
            var output = new OutputCursor(_record!);
            output.WriteByte((byte)kind);
            var written = kind == RecordKind.Remove ? RemoveWrittenFields : fields.Length;
            for (int f = 0; f < written; f++)
            {
                WriteField(value, fields[f].Type, starts[f], lengths[f], ref output);
            }
            return output.Position;
        }

        private static bool IsNull(ReadOnlySpan<byte> value, int start, int length)
        {
            return start < 0 || value.Slice(start, length).SequenceEqual("null"u8);
        }

        // Spans of the last occurrence of each field, relative to the object's value
        private static void FindFields(ReadOnlySpan<byte> value, Property[] fields, Span<int> starts, Span<int> lengths)
        {
            starts.Fill(-1);
            var reader = new Utf8JsonReader(value);
            reader.Read();
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                var field = -1;
                for (int f = 0; f < fields.Length; f++)
                {
                    if (reader.ValueTextEquals(fields[f].Name))
                    {
                        field = f;
                        break;
                    }
                }
                reader.Read();
                var start = (int)reader.TokenStartIndex;
                reader.Skip();
                if (field >= 0)
                {
                    starts[field] = start;
                    lengths[field] = (int)reader.BytesConsumed - start;
                }
            }
        }

        private void WriteField(ReadOnlySpan<byte> value, Field type, int start, int length, ref OutputCursor output)
        {
            if (start < 0)
            {
                WriteAbsent(type, ref output);
                return;
            }
            var fieldValue = value.Slice(start, length);
            var reader = new Utf8JsonReader(fieldValue);
            reader.Read();
            if (reader.TokenType == JsonTokenType.Null)
            {
                output.WriteByte(0);
                return;
            }
            switch (type)
            {
                case Field.String:
                    output.WriteJsonString(ref reader);
                    break;
                case Field.Long:
                case Field.NullableLong:
                    output.WriteByte(1);
                    output.WriteVarint(RecordWriter.Zigzag(reader.GetInt64()));
                    break;
                case Field.Int:
                    output.WriteByte(1);
                    output.WriteVarint(RecordWriter.Zigzag(reader.GetInt32()));
                    break;
                case Field.Bool:
                case Field.NullableBool:
                    output.WriteByte(reader.TokenType == JsonTokenType.True ? (byte)2 : (byte)1);
                    break;
                case Field.Map:
                    WriteMap(fieldValue, ref output);
                    break;
                case Field.List:
                    WriteList(ref reader, ref output);
                    break;
                case Field.Vector:
                case Field.Format:
                    var nested = type == Field.Vector ? s_vector : s_format;
                    Span<int> starts = stackalloc int[nested.Length];
                    Span<int> lengths = stackalloc int[nested.Length];
                    FindFields(fieldValue, nested, starts, lengths);
                    output.WriteByte(1);
                    for (int f = 0; f < nested.Length; f++)
                    {
                        WriteField(fieldValue, nested[f].Type, starts[f], lengths[f], ref output);
                    }
                    break;
            }
        }

        // An absent field reads as the model's default: zero and false for value types, null otherwise
        private static void WriteAbsent(Field type, ref OutputCursor output)
        {
            switch (type)
            {
                case Field.Long:
                case Field.Int:
                    output.WriteByte(1);
                    output.WriteVarint(0);
                    break;
                case Field.Bool:
                    output.WriteByte(1);
                    break;
                default:
                    output.WriteByte(0);
                    break;
            }
        }

        private static void WriteList(ref Utf8JsonReader reader, ref OutputCursor output)
        {
            var probe = reader;
            var count = 0;
            while (probe.Read() && probe.TokenType != JsonTokenType.EndArray)
            {
                count++;
            }
            output.WriteVarint((ulong)count + 1);
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                if (reader.TokenType == JsonTokenType.Null)
                {
                    output.WriteByte(0);
                }
                else
                {
                    output.WriteJsonString(ref reader);
                }
            }
        }

        // For one map: index the unescaped keys, mark superseded entries, then write the survivors in order
        private void WriteMap(ReadOnlySpan<byte> map, ref OutputCursor output)
        {
            var entries = MemoryMarshal.Cast<byte, KeyEntry>(_keyIndex.AsSpan());
            var reader = new Utf8JsonReader(map);
            reader.Read();
            var count = 0;
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                var keyStart = (int)reader.TokenStartIndex;
                var key = Key(map.Slice(keyStart), _keyScratch);
                entries[count] = new KeyEntry() { Hash = Hash(key), Start = keyStart, Ordinal = count };
                count++;
                reader.Read();
                reader.Skip();
            }
            var superseded = _keyIndex.AsSpan(count * Marshal.SizeOf<KeyEntry>(), (count + 7) / 8);
            superseded.Clear();
            var survivors = count;
            if (count > 1)
            {
                var sorted = entries.Slice(0, count);
                sorted.Sort(static (x, y) => x.Hash != y.Hash ? x.Hash.CompareTo(y.Hash) : x.Ordinal.CompareTo(y.Ordinal));
                for (int i = 0; i < count - 1; i++)
                {
                    for (int j = i + 1; j < count && sorted[j].Hash == sorted[i].Hash; j++)
                    {
                        if (SameKey(map, sorted[i], sorted[j]))
                        {
                            superseded[sorted[i].Ordinal / 8] |= (byte)(1 << (sorted[i].Ordinal % 8));
                            survivors--;
                            break;
                        }
                    }
                }
            }

            output.WriteVarint((ulong)survivors + 1);
            reader = new Utf8JsonReader(map);
            reader.Read();
            var ordinal = 0;
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                var skip = (superseded[ordinal / 8] & (1 << (ordinal % 8))) != 0;
                ordinal++;
                if (skip)
                {
                    reader.Read();
                    reader.Skip();
                    continue;
                }
                output.WriteJsonString(ref reader);
                reader.Read();
                if (reader.TokenType == JsonTokenType.Null)
                {
                    output.WriteByte(0);
                }
                else
                {
                    output.WriteJsonString(ref reader);
                }
            }
        }

        private bool SameKey(ReadOnlySpan<byte> map, KeyEntry x, KeyEntry y)
        {
            var left = Key(map.Slice(x.Start), _keyScratch);
            var used = _keyScratch != null && left.Overlaps(_keyScratch) ? left.Length : 0;
            var right = Key(map.Slice(y.Start), _keyScratch.AsSpan(used));
            return left.SequenceEqual(right);
        }

        // The unescaped key whose quoted token starts the span; escaped keys are copied into the scratch
        private static ReadOnlySpan<byte> Key(ReadOnlySpan<byte> fromToken, Span<byte> scratch)
        {
            var reader = new Utf8JsonReader(fromToken, isFinalBlock: false, state: default);
            reader.Read();
            if (!reader.ValueIsEscaped)
            {
                return fromToken.Slice(1, reader.ValueSpan.Length);
            }
            var length = reader.CopyString(scratch);
            return scratch.Slice(0, length);
        }

        private static int Hash(ReadOnlySpan<byte> key)
        {
            var hash = new HashCode();
            hash.AddBytes(key);
            return hash.ToHashCode();
        }

        /// <summary>
        /// Writes into the reserved record buffer.
        /// </summary>
        private ref struct OutputCursor
        {
            private readonly Span<byte> _buffer;
            private int _position;

            public OutputCursor(Span<byte> buffer)
            {
                _buffer = buffer;
                _position = 0;
            }

            public readonly int Position => _position;

            public void WriteByte(byte value)
            {
                _buffer[_position++] = value;
            }

            public void WriteVarint(ulong value)
            {
                while (value >= 0x80)
                {
                    _buffer[_position++] = (byte)(value | 0x80);
                    value >>= 7;
                }
                _buffer[_position++] = (byte)value;
            }

            // Unescapes in place after room for the longest prefix, then moves left if the prefix came out shorter
            public void WriteJsonString(ref Utf8JsonReader reader)
            {
                var raw = reader.ValueSpan;
                if (!reader.ValueIsEscaped)
                {
                    WriteVarint((ulong)raw.Length + 1);
                    raw.CopyTo(_buffer.Slice(_position));
                    _position += raw.Length;
                    return;
                }
                var room = VarintLength((ulong)raw.Length + 1);
                var length = reader.CopyString(_buffer.Slice(_position + room));
                if (!Utf8.IsValid(_buffer.Slice(_position + room, length)))
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.InvalidUtf8, DeclineDependency.Tail, "An escaped string is not valid UTF-8");
                }
                var prefix = VarintLength((ulong)length + 1);
                if (prefix < room)
                {
                    _buffer.Slice(_position + room, length).CopyTo(_buffer.Slice(_position + prefix));
                }
                WriteVarint((ulong)length + 1);
                _position += length;
            }

            private static int VarintLength(ulong value)
            {
                var length = 1;
                while (value >= 0x80)
                {
                    value >>= 7;
                    length++;
                }
                return length;
            }
        }
    }
}
