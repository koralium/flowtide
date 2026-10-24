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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using System.Buffers.Binary;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// One orphan inventory's frozen candidates and the facts they were frozen at; disposing deletes the candidate run.
    /// </summary>
    internal sealed class OrphanInventory : IDisposable
    {
        // Sealed, of the candidate role and the identity's domain
        public required PathSet Candidates { get; init; }

        // Made the candidate keys; nothing owed at hand-over
        public required PathCanonicalizer Identity { get; init; }

        // The sweep reference file's modification time
        public required DateTimeOffset T0 { get; init; }

        // T0 less the retention and one hour, saturated
        public required DateTimeOffset Cutoff { get; init; }

        // The published version read just before the root listing
        public required long VInventory { get; init; }

        // The newest commit the root listing holds
        public required long H1 { get; init; }

        public required long O { get; init; }

        public required long A { get; init; }

        public required string TableId { get; init; }

        // Preparation's estimate of the protection reads
        public required long ProtectionReads { get; init; }

        public required OrphanCounts Counts { get; init; }

        public void Dispose() => Candidates.Dispose();
    }

    /// <summary>
    /// What one orphan inventory listed, excluded by each rule, and froze.
    /// </summary>
    internal readonly record struct OrphanCounts(
        // Entries the root listing delivered
        long Listed,
        // Entries after the synthetic folder block
        long Provider,
        // The sweep reference file
        long Reference,
        // Entries with a _delta_log segment
        long Log,
        // Entries inside a nested table
        long Nested,
        // Nested table roots discovered
        long NestedRoots,
        // Entries with a hidden segment
        long Hidden,
        // Folders and folder placeholders
        long Folder,
        // Entries without a modification time
        long NoTime,
        // Entries modified at or after the cutoff
        long Young,
        // Pinned Azure entries without a positive size
        long UnknownSize,
        // Entries without a qualified deletion path
        long Unqualified,
        // Entries frozen as candidates
        long Frozen);

    /// <summary>
    /// A frozen candidate's payload: the size, modification time and MD5 the root listing reported.
    /// </summary>
    internal static class OrphanCandidatePayload
    {
        // Size, modification ticks and the MD5 byte count
        private const int HeaderLength = 17;

        // A longer MD5 is stored as absent
        public const int MaxMd5Bytes = byte.MaxValue;

        public const int MaxLength = HeaderLength + MaxMd5Bytes;

        /// <summary>
        /// Writes a payload into a destination of at least MaxLength bytes and returns its length.
        /// </summary>
        public static int Encode(Span<byte> destination, long? size, DateTimeOffset modified, string? md5)
        {
            BinaryPrimitives.WriteInt64LittleEndian(destination, size is >= 0 ? size.Value : -1);
            BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(8), modified.UtcTicks);
            var md5Bytes = string.IsNullOrEmpty(md5) ? 0 : Encoding.UTF8.GetByteCount(md5);
            if (md5Bytes > MaxMd5Bytes)
            {
                md5Bytes = 0;
            }
            destination[16] = (byte)md5Bytes;
            if (md5Bytes > 0)
            {
                Encoding.UTF8.GetBytes(md5, destination.Slice(HeaderLength));
            }
            return HeaderLength + md5Bytes;
        }

        /// <summary>
        /// Reads a payload Encode wrote; false for any other bytes.
        /// </summary>
        public static bool TryDecode(ReadOnlySpan<byte> payload, out long? size, out DateTimeOffset modified, out string? md5)
        {
            size = null;
            modified = default;
            md5 = null;
            if (payload.Length < HeaderLength || payload.Length != HeaderLength + payload[16])
            {
                return false;
            }
            var listed = BinaryPrimitives.ReadInt64LittleEndian(payload);
            var ticks = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(8));
            if (listed < -1 || ticks < DateTimeOffset.MinValue.UtcTicks || ticks > DateTimeOffset.MaxValue.UtcTicks)
            {
                return false;
            }
            size = listed < 0 ? null : listed;
            modified = new DateTimeOffset(ticks, TimeSpan.Zero);
            md5 = payload[16] == 0 ? null : Encoding.UTF8.GetString(payload.Slice(HeaderLength));
            return true;
        }
    }
}
