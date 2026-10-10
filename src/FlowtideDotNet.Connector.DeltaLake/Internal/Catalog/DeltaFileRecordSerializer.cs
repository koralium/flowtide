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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// Stateless, pages are serialized concurrently on eviction threads.
    /// </summary>
    internal sealed class DeltaFileRecordSerializer : IBplusTreeValueSerializer<DeltaFileRecord, ManagedValueContainer<DeltaFileRecord>>
    {
        private static readonly Func<DeltaFileRecord, int> s_sizeOf = x => x.ByteSize();

        public ManagedValueContainer<DeltaFileRecord> CreateEmpty()
        {
            return new ManagedValueContainer<DeltaFileRecord>(s_sizeOf);
        }

        public void Serialize(in IBufferWriter<byte> writer, in ManagedValueContainer<DeltaFileRecord> values)
        {
            BinaryWriting.WriteInt32(writer, values.Count);
            foreach (var record in values.Values)
            {
                Write(writer, record);
            }
        }

        public ManagedValueContainer<DeltaFileRecord> Deserialize(ref SequenceReader<byte> reader)
        {
            var count = BinaryReading.ReadInt32(ref reader);
            var container = CreateEmpty();
            for (int i = 0; i < count; i++)
            {
                container.Add(Read(ref reader));
            }
            return container;
        }

        internal static void Write(IBufferWriter<byte> writer, DeltaFileRecord record)
        {
            BinaryWriting.WriteString(writer, record.Path);
            BinaryWriting.WriteMap(writer, record.PartitionValues);
            BinaryWriting.WriteInt64(writer, record.Size);
            BinaryWriting.WriteInt64(writer, record.ModificationTime);
            BinaryWriting.WriteString(writer, record.Statistics);
            BinaryWriting.WriteMap(writer, record.Tags);
            var dv = record.DeletionVector;
            BinaryWriting.WriteBool(writer, dv != null);
            if (dv != null)
            {
                BinaryWriting.WriteString(writer, dv.StorageType);
                BinaryWriting.WriteString(writer, dv.PathOrInlineDv);
                BinaryWriting.WriteNullableInt64(writer, dv.Offset);
                BinaryWriting.WriteInt32(writer, dv.SizeInBytes);
                BinaryWriting.WriteInt64(writer, dv.Cardinality);
            }
            BinaryWriting.WriteNullableInt64(writer, record.BaseRowId);
            BinaryWriting.WriteNullableInt64(writer, record.DefaultRowCommitVersion);
            BinaryWriting.WriteString(writer, record.ClusteringProvider);
            BinaryWriting.WriteNullableInt64(writer, record.NumRecords);
        }

        internal static DeltaFileRecord Read(ref SequenceReader<byte> reader)
        {
            var path = BinaryReading.ReadString(ref reader) ?? throw new InvalidOperationException("A catalog record has no path");
            var partitionValues = BinaryReading.ReadMap(ref reader);
            var size = BinaryReading.ReadInt64(ref reader);
            var modificationTime = BinaryReading.ReadInt64(ref reader);
            var statistics = BinaryReading.ReadString(ref reader);
            var tags = BinaryReading.ReadMap(ref reader);
            DeletionVector? dv = null;
            if (BinaryReading.ReadBool(ref reader))
            {
                dv = new DeletionVector()
                {
                    StorageType = BinaryReading.ReadString(ref reader),
                    PathOrInlineDv = BinaryReading.ReadString(ref reader),
                    Offset = BinaryReading.ReadNullableInt64(ref reader),
                    SizeInBytes = BinaryReading.ReadInt32(ref reader),
                    Cardinality = BinaryReading.ReadInt64(ref reader)
                };
            }
            return new DeltaFileRecord()
            {
                Path = path,
                PartitionValues = partitionValues,
                Size = size,
                ModificationTime = modificationTime,
                Statistics = statistics,
                Tags = tags,
                DeletionVector = dv,
                BaseRowId = BinaryReading.ReadNullableInt64(ref reader),
                DefaultRowCommitVersion = BinaryReading.ReadNullableInt64(ref reader),
                ClusteringProvider = BinaryReading.ReadString(ref reader),
                NumRecords = BinaryReading.ReadNullableInt64(ref reader)
            };
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }
    }

    internal static class BinaryWriting
    {
        public static void WriteInt32(IBufferWriter<byte> writer, int value)
        {
            BinaryPrimitives.WriteInt32LittleEndian(writer.GetSpan(4), value);
            writer.Advance(4);
        }

        public static void WriteInt64(IBufferWriter<byte> writer, long value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(writer.GetSpan(8), value);
            writer.Advance(8);
        }

        public static void WriteBool(IBufferWriter<byte> writer, bool value)
        {
            writer.GetSpan(1)[0] = value ? (byte)1 : (byte)0;
            writer.Advance(1);
        }

        public static void WriteNullableInt64(IBufferWriter<byte> writer, long? value)
        {
            WriteBool(writer, value.HasValue);
            if (value.HasValue)
            {
                WriteInt64(writer, value.Value);
            }
        }

        // Length -1 is null
        public static void WriteString(IBufferWriter<byte> writer, string? value)
        {
            if (value == null)
            {
                WriteInt32(writer, -1);
                return;
            }
            var length = Encoding.UTF8.GetByteCount(value);
            WriteInt32(writer, length);
            var span = writer.GetSpan(length);
            Encoding.UTF8.GetBytes(value, span);
            writer.Advance(length);
        }

        // Count -1 is null, values may be null
        public static void WriteMap(IBufferWriter<byte> writer, Dictionary<string, string>? map)
        {
            if (map == null)
            {
                WriteInt32(writer, -1);
                return;
            }
            WriteInt32(writer, map.Count);
            foreach (var kv in map)
            {
                WriteString(writer, kv.Key);
                WriteString(writer, kv.Value);
            }
        }

        public static void WriteBytes(IBufferWriter<byte> writer, ReadOnlySpan<byte> bytes)
        {
            WriteInt32(writer, bytes.Length);
            bytes.CopyTo(writer.GetSpan(bytes.Length));
            writer.Advance(bytes.Length);
        }
    }

    internal static class BinaryReading
    {
        public static int ReadInt32(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int value))
            {
                throw new InvalidOperationException("Catalog page ended early");
            }
            return value;
        }

        public static long ReadInt64(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out long value))
            {
                throw new InvalidOperationException("Catalog page ended early");
            }
            return value;
        }

        public static bool ReadBool(ref SequenceReader<byte> reader)
        {
            if (!reader.TryRead(out var value))
            {
                throw new InvalidOperationException("Catalog page ended early");
            }
            return value != 0;
        }

        public static long? ReadNullableInt64(ref SequenceReader<byte> reader)
        {
            return ReadBool(ref reader) ? ReadInt64(ref reader) : null;
        }

        public static string? ReadString(ref SequenceReader<byte> reader)
        {
            var length = ReadInt32(ref reader);
            if (length < 0)
            {
                return null;
            }
            if (reader.UnreadSpan.Length >= length)
            {
                var text = Encoding.UTF8.GetString(reader.UnreadSpan.Slice(0, length));
                reader.Advance(length);
                return text;
            }
            var buffer = new byte[length];
            if (!reader.TryCopyTo(buffer))
            {
                throw new InvalidOperationException("Catalog page ended early");
            }
            reader.Advance(length);
            return Encoding.UTF8.GetString(buffer);
        }

        public static Dictionary<string, string>? ReadMap(ref SequenceReader<byte> reader)
        {
            var count = ReadInt32(ref reader);
            if (count < 0)
            {
                return null;
            }
            var map = new Dictionary<string, string>(count);
            for (int i = 0; i < count; i++)
            {
                var key = ReadString(ref reader)!;
                map[key] = ReadString(ref reader)!;
            }
            return map;
        }

        public static byte[] ReadBytes(ref SequenceReader<byte> reader)
        {
            var length = ReadInt32(ref reader);
            var bytes = new byte[length];
            if (!reader.TryCopyTo(bytes))
            {
                throw new InvalidOperationException("Catalog page ended early");
            }
            reader.Advance(length);
            return bytes;
        }
    }
}
