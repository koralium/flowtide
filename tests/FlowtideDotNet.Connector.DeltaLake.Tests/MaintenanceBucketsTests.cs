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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class MaintenanceBucketsTests
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 0, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_second = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(30);

        // Lists a fixed number of entries and supports nothing else
        internal sealed class ListingStub : IFileStorage
        {
            private readonly int _entries;

            public ListingStub(int entries)
            {
                _entries = entries;
            }

            public int Listings { get; private set; }

            public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
            {
                Listings++;
                IReadOnlyCollection<IOEntry> entries = Enumerable.Range(0, _entries).Select(i => new IOEntry($"/t/_delta_log/{i:D20}.json") { Size = 1 }).ToList();
                return Task.FromResult(entries);
            }

            public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task Rm(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
            public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

            public bool Disposed { get; private set; }

            public void Dispose()
            {
                Disposed = true;
            }
        }

        // Rates no test reaches, for tests about scheduling rather than metering
        internal static MaintenanceBuckets Unthrottled() => new MaintenanceBuckets(TimeProvider.System, 1L << 40, 1L << 40, 1L << 50, 1L << 50);

        // A rendezvous on state another task reaches, polled without holding the test's thread
        internal static async Task Until(Func<bool> condition)
        {
            var deadline = Environment.TickCount64 + (long)s_timeout.TotalMilliseconds;
            while (!condition())
            {
                Assert.True(Environment.TickCount64 < deadline, "The condition was never reached");
                await Task.Delay(1);
            }
        }

        [Fact]
        public async Task BucketsAreFifo()
        {
            var clock = new FakeTimeProvider(s_now);
            var bucket = new FifoTokenBucket(1, 10, clock);
            await bucket.AcquireAsync(10);

            var a = bucket.AcquireAsync(5).AsTask();
            var b = bucket.AcquireAsync(1).AsTask();
            var c = bucket.AcquireAsync(3).AsTask();
            Assert.Equal(3, bucket.QueueLength);

            // Two tokens would serve b, but a is ahead of it
            clock.Advance(2 * s_second);
            Assert.False(a.IsCompleted);
            Assert.False(b.IsCompleted);
            // An arrival the tokens would cover still waits behind the queue
            var d = bucket.AcquireAsync(1).AsTask();
            Assert.False(d.IsCompleted);

            clock.Advance(3 * s_second);
            Assert.True(a.IsCompleted);
            Assert.False(b.IsCompleted);
            clock.Advance(s_second);
            Assert.True(b.IsCompleted);
            Assert.False(c.IsCompleted);
            clock.Advance(3 * s_second);
            Assert.True(c.IsCompleted);
            Assert.False(d.IsCompleted);
            clock.Advance(s_second);
            Assert.True(d.IsCompleted);
            await Task.WhenAll(a, b, c, d);

            // A cancelled head lets the next waiter go as soon as its tokens are there
            using var cancellation = new CancellationTokenSource();
            var e = bucket.AcquireAsync(5, cancellation.Token).AsTask();
            var f = bucket.AcquireAsync(1).AsTask();
            clock.Advance(s_second);
            Assert.False(f.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => e);
            Assert.True(f.IsCompleted);
            Assert.Equal(0, bucket.QueueLength);
            Assert.Equal(10 + 5 + 1 + 3 + 1 + 1, bucket.Charged);
        }

        [Fact]
        public async Task ListingDebtRepaidOneTokenPerTurn()
        {
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 5);
            await buckets.Requests.AcquireAsync(5);
            clock.Advance(s_second);
            var storage = MeteredFileStorage.Wrap(new ListingStub(3001), buckets, ListingPaging.S3);

            // The listing's request takes the one token, so its three further pages are owed
            var listing = storage.Ls("/t/_delta_log/");
            Assert.False(listing.IsCompleted);
            Assert.Equal(3, buckets.Requests.Debt);
            // Another job's request arrives while the debt is owed
            var other = buckets.Requests.AcquireAsync(1).AsTask();

            clock.Advance(s_second);
            Assert.Equal(2, buckets.Requests.Debt);
            Assert.False(other.IsCompleted);

            // The listing's next turn rejoined the tail, so the other job goes first
            clock.Advance(s_second);
            Assert.True(other.IsCompleted);
            Assert.False(listing.IsCompleted);
            Assert.Equal(2, buckets.Requests.Debt);

            while (!listing.IsCompleted)
            {
                await Until(() => listing.IsCompleted || buckets.Requests.QueueLength == 1);
                if (!listing.IsCompleted)
                {
                    clock.Advance(s_second);
                }
            }
            Assert.Equal(3001, (await listing).Count);
            Assert.Equal(0, buckets.Requests.Debt);
            Assert.Equal(5 + 1 + 3 + 1, buckets.Requests.Charged);
        }

        [Theory]
        [InlineData("S3", 0, 0)]
        [InlineData("S3", 1000, 0)]
        [InlineData("S3", 1001, 1)]
        [InlineData("S3", 63500, 63)]
        [InlineData("Azure", 5000, 0)]
        [InlineData("Azure", 5001, 1)]
        [InlineData("Azure", 63500, 12)]
        [InlineData("LocalDisk", 0, 0)]
        [InlineData("LocalDisk", 100000, 0)]
        public async Task ListingDebtFollowsThePageSize(string paging, int entries, long extra)
        {
            var buckets = new MaintenanceBuckets(new FakeTimeProvider(s_now), requestBurst: 1000);
            var storage = MeteredFileStorage.Wrap(new ListingStub(entries), buckets, Enum.Parse<ListingPaging>(paging));

            Assert.Equal(entries, (await storage.Ls("/t/_delta_log/")).Count);

            // An empty listing still costs its one request
            Assert.Equal(1 + extra, buckets.Requests.Charged);
            Assert.Equal(0, buckets.Requests.Debt);
        }

        [Fact]
        public async Task CancellationPreservesIncurredListingDebt()
        {
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 5, bytesPerSecond: 1, byteBurst: 100);
            await buckets.Requests.AcquireAsync(5);
            clock.Advance(s_second);
            var storage = MeteredFileStorage.Wrap(new ListingStub(3001), buckets, ListingPaging.S3);
            using var cancellation = new CancellationTokenSource();

            var listing = storage.Ls("/t/_delta_log/", cancellationToken: cancellation.Token);
            clock.Advance(s_second);
            Assert.Equal(2, buckets.Requests.Debt);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => listing);

            // The two unpaid pages are still owed, by the bucket now
            Assert.Equal(2, buckets.Requests.Debt);
            Assert.Equal(5 + 1 + 3, buckets.Requests.Charged);
            var next = buckets.Requests.AcquireAsync(1).AsTask();
            clock.Advance(s_second);
            Assert.Equal(1, buckets.Requests.Debt);
            clock.Advance(s_second);
            Assert.Equal(0, buckets.Requests.Debt);
            Assert.False(next.IsCompleted);
            clock.Advance(s_second);
            Assert.True(next.IsCompleted);

            // Bytes a read already moved stay owed when the read's charge is cancelled
            await buckets.Bytes.AcquireAsync(100);
            using var read = new MeteredReadStream(new MemoryStream(new byte[40]), buckets);
            using var readCancellation = new CancellationTokenSource();
            var reading = read.ReadAsync(new byte[64], readCancellation.Token).AsTask();
            Assert.False(reading.IsCompleted);
            Assert.Equal(40, buckets.Bytes.Debt);
            readCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reading);
            Assert.Equal(40, buckets.Bytes.Debt);
            Assert.Equal(140, buckets.Bytes.Charged);
        }

        [Fact]
        public async Task MaintenanceLogListingsStopWaitingWhenCancelled()
        {
            var memo = DeclineMemo.For(new BuildResult(BuildStatus.Declined, Reason: MaintenanceReason.UnknownCheckpoint, Dependency: DeclineDependency.Root), 0, s_now)!;
            var listings = new (string Name, Func<IFileStorage, CancellationToken, Task> List)[]
            {
                ("transport", (storage, token) => new StowageCheckpointTransport(storage, "t").ListAsync(token)),
                ("memo", (storage, token) => memo.CheckAsync(storage, "t", 0, s_now, new JobMemory(), null, token)),
                ("segment", (storage, token) => DeltaLogSegment.ListAsync(storage, "t", token)),
                ("log", (storage, token) => DeltaTransactionReader.ListLog(storage, "t", token))
            };
            foreach (var (name, list) in listings)
            {
                // Waiting for the listing's own request
                var clock = new FakeTimeProvider(s_now);
                var buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 5);
                await buckets.Requests.AcquireAsync(5);
                var stub = new ListingStub(3001);
                using (var cancellation = new CancellationTokenSource())
                {
                    var waiting = list(MeteredFileStorage.Wrap(stub, buckets, ListingPaging.S3), cancellation.Token);
                    Assert.Equal(1, buckets.Requests.QueueLength);
                    cancellation.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting.WaitAsync(s_timeout));
                }
                Assert.Equal(0, stub.Listings);
                Assert.Equal(0, buckets.Requests.QueueLength);

                // Waiting for a turn to repay the listing's further pages; the unpaid pages stay owed
                clock.Advance(s_second);
                using (var cancellation = new CancellationTokenSource())
                {
                    var repaying = list(MeteredFileStorage.Wrap(stub, buckets, ListingPaging.S3), cancellation.Token);
                    Assert.Equal(3, buckets.Requests.Debt);
                    Assert.False(repaying.IsCompleted, name);
                    cancellation.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repaying.WaitAsync(s_timeout));
                }
                Assert.Equal(1, stub.Listings);
                Assert.Equal(3, buckets.Requests.Debt);
                Assert.Equal(0, buckets.Requests.QueueLength);
            }
        }

        [Fact]
        public async Task LargeObjectChargesBelowBurst()
        {
            const int MiB = 1 << 20;
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, bytesPerSecond: MiB, byteBurst: MiB);
            Assert.Equal(MiB, buckets.ByteChunk);
            var hooks = new HookFileStorage(Files.Of.InternalMemory("./buckets-" + Guid.NewGuid().ToString("N"))) { RecordWritesOf = "big.bin" };
            var storage = MeteredFileStorage.Wrap(hooks, buckets, ListingPaging.S3);
            var data = new byte[5 * MiB + 3];
            Random.Shared.NextBytes(data);

            // One write call of more than the burst is charged and written a chunk at a time
            var stream = await storage.OpenWrite("/t/big.bin");
            var write = stream.WriteAsync(data).AsTask();
            var advances = 0;
            while (!write.IsCompleted)
            {
                await Until(() => write.IsCompleted || buckets.Bytes.QueueLength == 1);
                if (write.IsCompleted)
                {
                    break;
                }
                clock.Advance(s_second);
                advances++;
            }
            await write;
            await stream.DisposeAsync();
            // The burst paid the first chunk; four more full chunks and the last three bytes each waited a second
            Assert.Equal(5, advances);
            Assert.Equal(MiB, buckets.Bytes.LargestTakeForTests);
            var writes = hooks.WriteEvents.Where(x => x.StartsWith('W')).Select(x => int.Parse(x.Substring(1))).ToList();
            Assert.All(writes, x => Assert.True(x <= MiB));
            Assert.Equal(data.Length, writes.Sum());

            // A read is never asked for more than a chunk, so its charge stays within the burst too
            var read = (await storage.OpenRead("/t/big.bin"))!;
            var copy = new MemoryStream();
            var buffer = new byte[8 * MiB];
            while (true)
            {
                var reading = read.ReadAsync(buffer).AsTask();
                while (!reading.IsCompleted)
                {
                    await Until(() => reading.IsCompleted || buckets.Bytes.QueueLength == 1);
                    if (!reading.IsCompleted)
                    {
                        clock.Advance(s_second);
                    }
                }
                var n = await reading;
                if (n == 0)
                {
                    break;
                }
                Assert.True(n <= MiB);
                copy.Write(buffer, 0, n);
            }
            await read.DisposeAsync();
            Assert.True(data.AsSpan().SequenceEqual(copy.ToArray()));
            Assert.Equal(MiB, buckets.Bytes.LargestTakeForTests);

            // A take above the burst could never be served, so it is refused rather than queued
            Assert.Throws<ArgumentOutOfRangeException>(() => { _ = buckets.Bytes.AcquireAsync(MiB + 1); });
            Assert.Equal(0, buckets.Bytes.QueueLength);
            // The default byte bucket charges at most 4 MiB at once
            Assert.Equal(4 * MiB, new MaintenanceBuckets(clock).ByteChunk);
        }
    }
}
