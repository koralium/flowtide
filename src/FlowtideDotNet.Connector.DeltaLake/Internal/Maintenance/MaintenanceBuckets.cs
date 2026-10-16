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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// How a store pages its listings, which sets the estimated pages behind a listing's entry count.
    /// </summary>
    internal enum ListingPaging : byte
    {
        // 1,000 entries a page; a store not known to page otherwise is estimated this way
        S3 = 0,
        // 5,000 entries a page, the qualified pinned Azure client
        Azure = 1,
        // One unpaged enumeration
        LocalDisk = 2
    }

    /// <summary>
    /// The process's maintenance request and byte buckets; foreground sink traffic is never charged.
    /// </summary>
    internal sealed class MaintenanceBuckets
    {
        public const long DefaultRequestsPerSecond = 200;
        public const long DefaultRequestBurst = 200;
        public const long DefaultBytesPerSecond = 64L << 20;
        public const long DefaultByteBurst = 64L << 20;

        // The largest byte charge taken at once
        public const long MaxByteChunk = 4L << 20;

        private long _nativeRequests;
        private long _nativeBytes;

        public MaintenanceBuckets(TimeProvider clock, long requestsPerSecond = DefaultRequestsPerSecond, long requestBurst = DefaultRequestBurst,
            long bytesPerSecond = DefaultBytesPerSecond, long byteBurst = DefaultByteBurst)
        {
            Requests = new FifoTokenBucket(requestsPerSecond, requestBurst, clock);
            Bytes = new FifoTokenBucket(bytesPerSecond, byteBurst, clock);
            ByteChunk = Math.Min(MaxByteChunk, byteBurst);
        }

        public FifoTokenBucket Requests { get; }

        public FifoTokenBucket Bytes { get; }

        // 4 MiB, or the byte burst when that is smaller
        public long ByteChunk { get; }

        // Charged for operations that bypass the storage, such as native renames
        public long NativeRequests => Interlocked.Read(ref _nativeRequests);

        public long NativeBytes => Interlocked.Read(ref _nativeBytes);

        /// <summary>
        /// The further pages a listing of this many entries is estimated to have cost beyond its first request.
        /// </summary>
        public static long ExtraPages(ListingPaging paging, long entries)
        {
            var perPage = paging switch
            {
                ListingPaging.LocalDisk => 0,
                ListingPaging.Azure => 5000,
                _ => 1000
            };
            return perPage == 0 ? 0 : Math.Max(0, (entries + perPage - 1) / perPage - 1);
        }

        /// <summary>
        /// Requests about to be issued, one FIFO turn each.
        /// </summary>
        public async ValueTask ChargeRequestsAsync(long count, CancellationToken cancellationToken)
        {
            for (long i = 0; i < count; i++)
            {
                await Requests.AcquireAsync(1, cancellationToken);
            }
        }

        /// <summary>
        /// Bytes about to move, taken in chunks no larger than <see cref="ByteChunk"/>.
        /// </summary>
        public async ValueTask ChargeBytesAsync(long bytes, CancellationToken cancellationToken)
        {
            while (bytes > 0)
            {
                var chunk = Math.Min(bytes, ByteChunk);
                await Bytes.AcquireAsync(chunk, cancellationToken);
                bytes -= chunk;
            }
        }

        /// <summary>
        /// Requests already made, such as a listing's later pages, repaid one token per FIFO turn.
        /// </summary>
        public Task RepayRequestsAsync(long count, CancellationToken cancellationToken) => Requests.RepayAsync(count, 1, cancellationToken);

        /// <summary>
        /// Bytes already moved, repaid in chunks no larger than <see cref="ByteChunk"/>.
        /// </summary>
        public Task RepayBytesAsync(long bytes, CancellationToken cancellationToken) => Bytes.RepayAsync(bytes, ByteChunk, cancellationToken);

        public void RepayBytes(long bytes) => Bytes.Repay(bytes, ByteChunk);

        /// <summary>
        /// A native operation that bypasses the storage: its requests and bytes are charged before it runs.
        /// </summary>
        public async ValueTask ChargeNativeAsync(long requests, long bytes, CancellationToken cancellationToken)
        {
            await ChargeRequestsAsync(requests, cancellationToken);
            await ChargeBytesAsync(bytes, cancellationToken);
            Interlocked.Add(ref _nativeRequests, requests);
            Interlocked.Add(ref _nativeBytes, bytes);
        }
    }
}
