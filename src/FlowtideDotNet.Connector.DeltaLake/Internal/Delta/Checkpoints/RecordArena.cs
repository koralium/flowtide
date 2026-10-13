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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// One keyed record tagged with the version and ordinal it was read at, as stored in the arena and in sort runs.
    /// </summary>
    internal readonly ref struct ArenaEntry
    {
        public ArenaEntry(ReadOnlySpan<byte> key, long version, long ordinal, ReadOnlySpan<byte> record, int length)
        {
            Key = key;
            Version = version;
            Ordinal = ordinal;
            Record = record;
            Length = length;
        }

        public ReadOnlySpan<byte> Key { get; }

        public long Version { get; }

        public long Ordinal { get; }

        public ReadOnlySpan<byte> Record { get; }

        // The encoded size of the whole entry
        public int Length { get; }

        // Later entries for a key come first: version, then ordinal, descending
        public static int Compare(in ArenaEntry x, in ArenaEntry y)
        {
            var keys = x.Key.SequenceCompareTo(y.Key);
            if (keys != 0)
            {
                return keys;
            }
            var versions = y.Version.CompareTo(x.Version);
            return versions != 0 ? versions : y.Ordinal.CompareTo(x.Ordinal);
        }

        public static int EncodedLength(int key, int record) => Varint.Length((uint)key) + Varint.Length((uint)record) + key + 16 + record;

        // Both lengths lead, so the entry's size is known from its first bytes
        public static int Encode(Span<byte> destination, ReadOnlySpan<byte> key, long version, long ordinal, ReadOnlySpan<byte> record)
        {
            var position = Varint.Write(destination, (uint)key.Length);
            position += Varint.Write(destination.Slice(position), (uint)record.Length);
            key.CopyTo(destination.Slice(position));
            position += key.Length;
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(position), version);
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(position + 8), ordinal);
            position += 16;
            record.CopyTo(destination.Slice(position));
            return position + record.Length;
        }

        /// <summary>
        /// Decodes the entry at the start of the span; false when the span ends inside it.
        /// </summary>
        public static bool TryDecode(ReadOnlySpan<byte> source, out ArenaEntry entry)
        {
            entry = default;
            if (!TryReadLengths(source, out var keyLength, out var recordLength, out var position) || source.Length < (long)position + keyLength + 16 + recordLength)
            {
                return false;
            }
            var key = source.Slice(position, (int)keyLength);
            position += (int)keyLength;
            var version = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(position));
            var ordinal = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(position + 8));
            position += 16;
            entry = new ArenaEntry(key, version, ordinal, source.Slice(position, (int)recordLength), position + (int)recordLength);
            return true;
        }

        /// <summary>
        /// The length of the entry at the start of the span once its two lengths are there, else null.
        /// </summary>
        public static long? RequiredLength(ReadOnlySpan<byte> source) =>
            TryReadLengths(source, out var keyLength, out var recordLength, out var position) ? (long)position + keyLength + 16 + recordLength : null;

        private static bool TryReadLengths(ReadOnlySpan<byte> source, out uint keyLength, out uint recordLength, out int position)
        {
            recordLength = 0;
            if (!TryReadVarint(source, out keyLength, out position) || !TryReadVarint(source.Slice(position), out recordLength, out var recordBytes))
            {
                return false;
            }
            position += recordBytes;
            return true;
        }

        private static bool TryReadVarint(ReadOnlySpan<byte> source, out uint value, out int length)
        {
            value = 0;
            for (length = 0; length < 5 && length < source.Length; length++)
            {
                var b = source[length];
                value |= (uint)(b & 0x7F) << (7 * length);
                if ((b & 0x80) == 0)
                {
                    length++;
                    return true;
                }
            }
            if (length == 5)
            {
                throw new InvalidDataException("A run varint is too long");
            }
            return false;
        }
    }

    /// <summary>
    /// Entries in 1 MiB chunks from the job's managed ledger; an entry larger than a chunk gets a chunk of its own.
    /// </summary>
    internal sealed class RecordArena : IDisposable
    {
        public const int ChunkSize = 1 << 20;

        private readonly JobMemory _memory;
        private readonly int _chunkSize;
        private readonly List<byte[]> _chunks = new List<byte[]>();
        private int _used;

        // Chunks of an eighth of the allowance, at most 1 MiB, so a small allowance still holds several entries
        public RecordArena(JobMemory memory, long allowance = TailReducer.DefaultArenaCap)
        {
            _memory = memory;
            _chunkSize = (int)Math.Clamp(allowance / 8, 4096, ChunkSize);
        }

        // Bytes of every chunk held
        public long Bytes { get; private set; }

        // The chunk bytes appending this entry would add
        public long GrowthFor(int key, int record)
        {
            var length = ArenaEntry.EncodedLength(key, record);
            return _chunks.Count > 0 && _chunks[^1].Length - _used >= length ? 0 : Math.Max(_chunkSize, length);
        }

        /// <summary>
        /// Appends an entry and returns its location: the chunk in the high half, the offset in the low half.
        /// </summary>
        public long Append(ReadOnlySpan<byte> key, long version, long ordinal, ReadOnlySpan<byte> record)
        {
            var length = ArenaEntry.EncodedLength(key.Length, record.Length);
            if (_chunks.Count == 0 || _chunks[^1].Length - _used < length)
            {
                _chunks.Add(_memory.Allocate(Math.Max(_chunkSize, length)));
                Bytes += _chunks[^1].Length;
                _used = 0;
            }
            var chunk = _chunks.Count - 1;
            var offset = _used;
            _used += ArenaEntry.Encode(_chunks[chunk].AsSpan(offset), key, version, ordinal, record);
            return ((long)chunk << 32) | (uint)offset;
        }

        public ArenaEntry Read(long location)
        {
            var chunk = _chunks[(int)(location >> 32)];
            ArenaEntry.TryDecode(chunk.AsSpan((int)(uint)location), out var entry);
            return entry;
        }

        // The encoded bytes of the entry at a location
        public ReadOnlySpan<byte> Raw(long location)
        {
            var entry = Read(location);
            return _chunks[(int)(location >> 32)].AsSpan((int)(uint)location, entry.Length);
        }

        public void Clear()
        {
            foreach (var chunk in _chunks)
            {
                _memory.Free(chunk);
            }
            _chunks.Clear();
            _used = 0;
            Bytes = 0;
        }

        public void Dispose() => Clear();
    }
}
