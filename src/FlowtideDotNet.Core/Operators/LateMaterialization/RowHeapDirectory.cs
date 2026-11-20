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

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal sealed class RowHeapDirectory
    {
        public const int ChunkBits = 12;
        public const int ChunkSize = 1 << ChunkBits;

        private long[]?[] _chunks;
        private int _chunkCount;

        public RowHeapDirectory()
        {
            _chunks = new long[]?[16];
        }

        public long ByteSize => ((long)Volatile.Read(ref _chunkCount) * ChunkSize + Volatile.Read(ref _chunks).Length) * sizeof(long);

        // Zero when the sequence has no page
        public long Get(long sequence)
        {
            var chunks = Volatile.Read(ref _chunks);
            long chunkIndex = sequence >> ChunkBits;
            // One compare for below zero and beyond the table
            if ((ulong)chunkIndex >= (ulong)chunks.Length)
            {
                return 0;
            }
            var chunk = Volatile.Read(ref chunks[chunkIndex]);
            if (chunk == null)
            {
                return 0;
            }
            return Volatile.Read(ref chunk[sequence & (ChunkSize - 1)]);
        }

        // Called by the one thread that appends
        public void Set(long sequence, long pageId)
        {
            long chunkIndex = sequence >> ChunkBits;
            if (sequence < 1 || chunkIndex >= Array.MaxLength)
            {
                throw new InvalidOperationException($"The row heap directory has no slot for page sequence {sequence}.");
            }
            var chunks = _chunks;
            if (chunkIndex >= chunks.Length)
            {
                var larger = new long[]?[Math.Max(chunks.Length * 2, (int)chunkIndex + 1)];
                Array.Copy(chunks, larger, chunks.Length);
                // The chunks are shared, so a reader of the old table loses nothing
                Volatile.Write(ref _chunks, larger);
                chunks = larger;
            }
            var chunk = chunks[chunkIndex];
            if (chunk == null)
            {
                chunk = new long[ChunkSize];
                Volatile.Write(ref chunks[chunkIndex], chunk);
                Volatile.Write(ref _chunkCount, _chunkCount + 1);
            }
            Volatile.Write(ref chunk[sequence & (ChunkSize - 1)], pageId);
        }
    }
}
