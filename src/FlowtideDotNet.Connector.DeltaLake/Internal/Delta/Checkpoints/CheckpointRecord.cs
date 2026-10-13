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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using System.Buffers.Text;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// The action a record holds, stored in the low bits of its first byte.
    /// </summary>
    internal enum RecordKind : byte
    {
        Protocol = 0,
        MetaData = 1,
        Txn = 2,
        Domain = 3,
        Add = 4,
        Remove = 5
    }

    /// <summary>
    /// The reconciliation family, the first byte of a key; protocol and metaData sort first.
    /// </summary>
    internal enum RecordFamily : byte
    {
        Protocol = 0,
        MetaData = 1,
        Txn = 2,
        Domain = 3,
        File = 4
    }

    /// <summary>
    /// A checkpointed action as a versioned binary row in fixed field order.
    /// </summary>
    internal static class CheckpointRecord
    {
        public const byte ParsedOnlyFlag = 0x80;

        private const byte KindMask = 0x0F;

        public static RecordKind Kind(ReadOnlySpan<byte> record) => (RecordKind)(record[0] & KindMask);

        public static bool ParsedOnly(ReadOnlySpan<byte> record) => (record[0] & ParsedOnlyFlag) != 0;

        // A remove's deletion timestamp, the field after its path
        public static long? DeletionTimestamp(ReadOnlySpan<byte> record)
        {
            var r = new RecordReader(record);
            r.ReadKind(out _);
            r.SkipString();
            return r.ReadLong();
        }

        // Whether a domain record is a removal, its third field
        public static bool DomainRemoved(ReadOnlySpan<byte> record)
        {
            var r = new RecordReader(record);
            r.ReadKind(out _);
            r.SkipString();
            r.SkipString();
            return r.ReadBool() == true;
        }

        // A txn record's version, the field after its app id
        public static long? TxnVersion(ReadOnlySpan<byte> record)
        {
            var r = new RecordReader(record);
            r.ReadKind(out _);
            r.SkipString();
            return r.ReadLong();
        }

        public static RecordFamily Family(RecordKind kind) => kind switch
        {
            RecordKind.Protocol => RecordFamily.Protocol,
            RecordKind.MetaData => RecordFamily.MetaData,
            RecordKind.Txn => RecordFamily.Txn,
            RecordKind.Domain => RecordFamily.Domain,
            _ => RecordFamily.File
        };

        /// <summary>
        /// The reader's model of a record; null fields of non-nullable model properties read as their defaults.
        /// </summary>
        public static DeltaAction ToAction(ReadOnlySpan<byte> record)
        {
            var r = new RecordReader(record);
            var kind = r.ReadKind(out _);
            switch (kind)
            {
                case RecordKind.Add:
                    return new DeltaAction()
                    {
                        Add = new DeltaAddAction()
                        {
                            Path = r.ReadString(),
                            PartitionValues = r.ReadMap(),
                            Size = r.ReadLong() ?? 0,
                            ModificationTime = r.ReadLong() ?? 0,
                            DataChange = r.ReadBool() ?? false,
                            Statistics = r.ReadString(),
                            Tags = r.ReadMap(),
                            DeletionVector = ReadVector(ref r),
                            BaseRowId = r.ReadLong(),
                            DefaultRowCommitVersion = r.ReadLong(),
                            ClusteringProvider = r.ReadString()
                        }
                    };
                case RecordKind.Remove:
                    return new DeltaAction()
                    {
                        Remove = new DeltaRemoveFileAction()
                        {
                            Path = r.ReadString(),
                            DeletionTimestamp = r.ReadLong(),
                            DataChange = r.ReadBool() ?? false,
                            ExtendedFileMetadata = r.ReadBool() ?? false,
                            PartitionValues = r.ReadMap(),
                            Size = r.ReadLong(),
                            DeletionVector = ReadVector(ref r),
                            BaseRowId = r.ReadLong(),
                            DefaultRowCommitVersion = r.ReadLong()
                        }
                    };
                case RecordKind.MetaData:
                    var metaData = new DeltaMetadataAction()
                    {
                        Id = r.ReadString(),
                        Name = r.ReadString(),
                        Description = r.ReadString()
                    };
                    if (r.ReadStruct())
                    {
                        metaData.Format = new DeltaMetadataFormat() { Provider = r.ReadString(), Options = r.ReadMap() };
                    }
                    metaData.SchemaString = r.ReadString();
                    metaData.PartitionColumns = r.ReadList();
                    metaData.CreatedTime = r.ReadLong();
                    metaData.Configuration = r.ReadMap();
                    return new DeltaAction() { MetaData = metaData };
                case RecordKind.Protocol:
                    return new DeltaAction()
                    {
                        Protocol = new DeltaProtocolAction()
                        {
                            MinReaderVersion = (int)(r.ReadLong() ?? 0),
                            MinWriterVersion = (int)(r.ReadLong() ?? 0),
                            ReaderFeatures = r.ReadList(),
                            WriterFeatures = r.ReadList()
                        }
                    };
                case RecordKind.Txn:
                    return new DeltaAction()
                    {
                        Txn = new DeltaTransactionAction() { AppId = r.ReadString(), Version = r.ReadLong() ?? 0, LastUpdated = r.ReadLong() }
                    };
                case RecordKind.Domain:
                    return new DeltaAction()
                    {
                        DomainMetadata = new DeltaDomainMetadataAction() { Domain = r.ReadString(), Configuration = r.ReadString(), Removed = r.ReadBool() ?? false }
                    };
                default:
                    throw new InvalidDataException($"Unknown record kind {kind}");
            }
        }

        private static DeletionVector? ReadVector(ref RecordReader r)
        {
            if (!r.ReadStruct())
            {
                return null;
            }
            return new DeletionVector()
            {
                StorageType = r.ReadString(),
                PathOrInlineDv = r.ReadString(),
                Offset = r.ReadLong(),
                SizeInBytes = (int)(r.ReadLong() ?? 0),
                Cardinality = r.ReadLong() ?? 0
            };
        }
    }

    /// <summary>
    /// Writes record fields into a span sized beforehand, or only counts them.
    /// </summary>
    internal ref struct RecordWriter
    {
        private readonly Span<byte> _destination;
        private readonly bool _counting;
        private int _position;

        public RecordWriter(Span<byte> destination)
        {
            _destination = destination;
            _counting = false;
            _position = 0;
        }

        private RecordWriter(bool counting)
        {
            _destination = default;
            _counting = counting;
            _position = 0;
        }

        public static RecordWriter Counting() => new RecordWriter(true);

        public readonly int Position => _position;

        public void WriteKind(RecordKind kind, bool parsedOnly)
        {
            WriteByte((byte)((byte)kind | (parsedOnly ? CheckpointRecord.ParsedOnlyFlag : 0)));
        }

        public void WriteByte(byte value)
        {
            if (!_counting)
            {
                _destination[_position] = value;
            }
            _position++;
        }

        public void WriteVarint(ulong value)
        {
            while (value >= 0x80)
            {
                WriteByte((byte)(value | 0x80));
                value >>= 7;
            }
            WriteByte((byte)value);
        }

        public void WriteBytes(scoped ReadOnlySpan<byte> bytes)
        {
            if (!_counting)
            {
                bytes.CopyTo(_destination.Slice(_position));
            }
            _position += bytes.Length;
        }

        public void WriteString(scoped ReadOnlySpan<byte> utf8)
        {
            WriteVarint((ulong)utf8.Length + 1);
            WriteBytes(utf8);
        }

        public void WriteNull()
        {
            WriteByte(0);
        }

        public void WriteString(string? value)
        {
            if (value == null)
            {
                WriteNull();
                return;
            }
            var length = Encoding.UTF8.GetByteCount(value);
            WriteVarint((ulong)length + 1);
            if (!_counting)
            {
                Encoding.UTF8.GetBytes(value, _destination.Slice(_position, length));
            }
            _position += length;
        }

        public void WriteLong(long? value)
        {
            if (value == null)
            {
                WriteNull();
                return;
            }
            WriteByte(1);
            WriteVarint(Zigzag(value.Value));
        }

        public void WriteBool(bool? value)
        {
            WriteByte(value == null ? (byte)0 : value.Value ? (byte)2 : (byte)1);
        }

        public void WriteStruct(bool present)
        {
            WriteByte(present ? (byte)1 : (byte)0);
        }

        // Maps and lists: the count, then the entries
        public void WriteCount(int? count)
        {
            WriteVarint(count == null ? 0 : (ulong)count.Value + 1);
        }

        public void WriteMap(IReadOnlyDictionary<string, string>? map)
        {
            if (map == null)
            {
                WriteNull();
                return;
            }
            WriteCount(map.Count);
            foreach (var entry in map)
            {
                WriteString(entry.Key);
                WriteString(entry.Value);
            }
        }

        public void WriteList(IReadOnlyList<string>? list)
        {
            if (list == null)
            {
                WriteNull();
                return;
            }
            WriteCount(list.Count);
            for (int i = 0; i < list.Count; i++)
            {
                WriteString(list[i]);
            }
        }

        public static ulong Zigzag(long value) => (ulong)((value << 1) ^ (value >> 63));
    }

    /// <summary>
    /// Reads record fields in the order they were written; malformed input throws <see cref="InvalidDataException"/>.
    /// </summary>
    internal ref struct RecordReader
    {
        private readonly ReadOnlySpan<byte> _record;
        private int _position;

        public RecordReader(ReadOnlySpan<byte> record)
        {
            _record = record;
            _position = 0;
        }

        public readonly int Position => _position;

        public readonly bool End => _position == _record.Length;

        public RecordKind ReadKind(out bool parsedOnly)
        {
            var value = ReadByte();
            parsedOnly = (value & CheckpointRecord.ParsedOnlyFlag) != 0;
            return (RecordKind)(value & 0x0F);
        }

        public byte ReadByte()
        {
            if (_position >= _record.Length)
            {
                throw new InvalidDataException("The record ends early");
            }
            return _record[_position++];
        }

        public ulong ReadVarint()
        {
            ulong result = 0;
            for (int shift = 0; shift < 64; shift += 7)
            {
                var b = ReadByte();
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return result;
                }
            }
            throw new InvalidDataException("A varint is too long");
        }

        // False for null, the span then is empty
        public bool ReadUtf8(out ReadOnlySpan<byte> value)
        {
            var length = ReadVarint();
            if (length == 0)
            {
                value = default;
                return false;
            }
            var size = length - 1;
            if (size > (ulong)(_record.Length - _position))
            {
                throw new InvalidDataException("A string runs past the record");
            }
            value = _record.Slice(_position, (int)size);
            _position += (int)size;
            return true;
        }

        public string? ReadString()
        {
            return ReadUtf8(out var value) ? Encoding.UTF8.GetString(value) : null;
        }

        public long? ReadLong()
        {
            var present = ReadByte();
            if (present == 0)
            {
                return null;
            }
            var raw = ReadVarint();
            return (long)(raw >> 1) ^ -(long)(raw & 1);
        }

        public bool? ReadBool()
        {
            return ReadByte() switch
            {
                0 => null,
                1 => false,
                2 => true,
                var other => throw new InvalidDataException($"Invalid boolean {other}")
            };
        }

        public bool ReadStruct()
        {
            return ReadByte() switch
            {
                0 => false,
                1 => true,
                var other => throw new InvalidDataException($"Invalid struct flag {other}")
            };
        }

        public int? ReadCount()
        {
            var count = ReadVarint();
            if (count == 0)
            {
                return null;
            }
            // Every entry takes at least a byte, so a count past the remaining bytes is corrupt
            if (count - 1 > int.MaxValue || count - 1 > (ulong)(_record.Length - _position))
            {
                throw new InvalidDataException("A count is too large");
            }
            return (int)(count - 1);
        }

        public Dictionary<string, string>? ReadMap()
        {
            var count = ReadCount();
            if (count == null)
            {
                return null;
            }
            var map = new Dictionary<string, string>();
            for (int i = 0; i < count.Value; i++)
            {
                var key = ReadString()!;
                map[key] = ReadString()!;
            }
            return map;
        }

        public List<string>? ReadList()
        {
            var count = ReadCount();
            if (count == null)
            {
                return null;
            }
            var list = new List<string>(count.Value);
            for (int i = 0; i < count.Value; i++)
            {
                list.Add(ReadString()!);
            }
            return list;
        }

        public void SkipString()
        {
            ReadUtf8(out _);
        }

        public void SkipLong()
        {
            if (ReadByte() != 0)
            {
                ReadVarint();
            }
        }

        public void SkipMap()
        {
            var count = ReadCount() ?? 0;
            for (int i = 0; i < count; i++)
            {
                SkipString();
                SkipString();
            }
        }

        public void SkipList()
        {
            var count = ReadCount() ?? 0;
            for (int i = 0; i < count; i++)
            {
                SkipString();
            }
        }
    }

    /// <summary>
    /// The reconciliation key of a record, compared as ordinal bytes.
    /// </summary>
    internal static class RecordKey
    {
        public static int Length(ReadOnlySpan<byte> record)
        {
            var writer = RecordWriter.Counting();
            Write(record, ref writer);
            return writer.Position;
        }

        public static int Write(ReadOnlySpan<byte> record, Span<byte> destination)
        {
            var writer = new RecordWriter(destination);
            Write(record, ref writer);
            return writer.Position;
        }

        public static byte[] Of(ReadOnlySpan<byte> record)
        {
            var key = new byte[Length(record)];
            Write(record, key);
            return key;
        }

        private static void Write(ReadOnlySpan<byte> record, ref RecordWriter writer)
        {
            var r = new RecordReader(record);
            var kind = r.ReadKind(out _);
            var family = CheckpointRecord.Family(kind);
            writer.WriteByte((byte)family);
            if (family == RecordFamily.Protocol || family == RecordFamily.MetaData)
            {
                return;
            }
            // A null id stays apart from an empty one, as the reader keeps them
            var present = r.ReadUtf8(out var id);
            writer.WriteVarint(present ? (ulong)id.Length + 1 : 0);
            writer.WriteBytes(id);
            if (family != RecordFamily.File)
            {
                return;
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
                writer.WriteByte(0);
                return;
            }
            writer.WriteByte(1);
            r.ReadUtf8(out var storageType);
            r.ReadUtf8(out var pathOrInlineDv);
            var offset = r.ReadLong();
            // The same id as DeltaFileKey.DvId
            Span<byte> suffix = stackalloc byte[21];
            var suffixLength = 0;
            if (offset != null)
            {
                suffix[0] = (byte)'@';
                Utf8Formatter.TryFormat(offset.Value, suffix.Slice(1), out var digits);
                suffixLength = digits + 1;
            }
            writer.WriteVarint((ulong)(storageType.Length + pathOrInlineDv.Length + suffixLength));
            writer.WriteBytes(storageType);
            writer.WriteBytes(pathOrInlineDv);
            writer.WriteBytes(suffix.Slice(0, suffixLength));
        }
    }
}
