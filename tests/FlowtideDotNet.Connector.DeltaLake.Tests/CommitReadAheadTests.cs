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
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CommitReadAheadTests
    {
        // A window over the scripted opens of entries of the given sizes
        private static CommitReadAhead Window(ScriptedOpens opens, long[] sizes, int depth, TransferAllowance? transfer = null, Func<bool>? mayIssue = null, CancellationToken cancellationToken = default)
        {
            return new CommitReadAhead(opens, index => ScriptedOpens.Entry(index, sizes[index]), sizes.Length, depth, transfer, mayIssue ?? (() => true), cancellationToken);
        }

        private static long[] Sizes(int count, long size = 10) => Enumerable.Repeat(size, count).ToArray();

        private sealed class DisposedStream : MemoryStream
        {
            public volatile bool Disposed;

            protected override void Dispose(bool disposing)
            {
                Disposed = true;
                base.Dispose(disposing);
            }
        }

        private static void AssertIncreasing(IReadOnlyList<long> calls)
        {
            for (int i = 1; i < calls.Count; i++)
            {
                Assert.True(calls[i - 1] < calls[i], $"Open {calls[i]} was called after {calls[i - 1]}");
            }
        }

        [Fact]
        public async Task IssuesUpToTheDepthAheadInOrder()
        {
            var opens = new ScriptedOpens();
            var streams = Enumerable.Range(0, 10).Select(_ => new MemoryStream()).ToArray();
            await using var window = Window(opens, Sizes(10), depth: 4);

            var first = window.NextAsync();
            Assert.Equal(new long[] { 0, 1, 2, 3 }, opens.Calls);
            Assert.True(window.Outstanding <= 4);
            foreach (var index in new[] { 3, 2, 1 })
            {
                opens.Open(index).SetResult(streams[index]);
            }
            Assert.False(first.IsCompleted);
            opens.Open(0).SetResult(streams[0]);
            using (var opened = await first)
            {
                Assert.Same(streams[0], opened.Stream);
            }

            var second = window.NextAsync();
            Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, opens.Calls);
            Assert.True(window.Outstanding <= 4);
            using (var opened = await second)
            {
                Assert.Same(streams[1], opened.Stream);
            }
            for (int i = 2; i < 10; i++)
            {
                opens.Open(i).TrySetResult(streams[i]);
                using var opened = await window.NextAsync();
                Assert.Same(streams[i], opened.Stream);
                Assert.True(window.Outstanding <= 4);
            }
            AssertIncreasing(opens.Calls);
            Assert.Equal(Enumerable.Range(0, 10).Select(x => (long)x), opens.Calls);
        }

        [Fact]
        public async Task FailureIsThrownAtItsOwnTurnOnly()
        {
            var opens = new ScriptedOpens();
            await using var window = Window(opens, Sizes(4), depth: 4);

            var first = window.NextAsync();
            opens.Open(2).SetException(new IOException("open 2 failed"));
            opens.Open(0).SetResult(new MemoryStream());
            opens.Open(1).SetResult(new MemoryStream());
            opens.Open(3).SetResult(new MemoryStream());

            using (var opened = await first)
            {
                Assert.NotNull(opened.Stream);
            }
            using (var opened = await window.NextAsync())
            {
                Assert.NotNull(opened.Stream);
            }
            var failure = await Assert.ThrowsAsync<IOException>(async () => await window.NextAsync());
            Assert.Equal("open 2 failed", failure.Message);
        }

        [Fact]
        public async Task MissingObjectIsNullAtItsTurn()
        {
            var opens = new ScriptedOpens();
            var transfer = new TransferAllowance(1 << 20);
            await using var window = Window(opens, Sizes(2), depth: 2, transfer);
            opens.Open(0).SetResult(null);
            opens.Open(1).SetResult(new MemoryStream());

            using (var opened = await window.NextAsync())
            {
                Assert.Null(opened.Stream);
                Assert.False(opened.Stopped);
                Assert.NotNull(opened.Admission);
            }
            using (var opened = await window.NextAsync())
            {
                Assert.NotNull(opened.Stream);
            }
            Assert.Equal(0, transfer.Charged);
        }

        [Fact]
        public async Task DisposeCancelsAwaitsAndReleasesEveryOutstandingOpen()
        {
            var opens = new ScriptedOpens();
            var transfer = new TransferAllowance(1 << 20);
            var window = Window(opens, Sizes(10), depth: 4, transfer);
            opens.Open(0).SetResult(new MemoryStream());
            using (var opened = await window.NextAsync())
            {
                Assert.NotNull(opened.Stream);
            }
            Assert.Equal(new long[] { 0, 1, 2, 3 }, opens.Calls);
            Assert.Equal(30, transfer.Charged);

            var disposing = window.DisposeAsync().AsTask();
            await MaintenanceBucketsTests.Until(() => opens.TokenOf(1).IsCancellationRequested);
            Assert.All(new[] { 1, 2, 3 }, index => Assert.True(opens.TokenOf(index).IsCancellationRequested));
            Assert.False(disposing.IsCompleted);

            var tracked = new DisposedStream();
            opens.Open(1).SetResult(tracked);
            opens.Open(2).SetException(new IOException("open 2 failed"));
            Assert.False(disposing.IsCompleted);
            opens.Open(3).SetResult(null);
            await disposing;

            Assert.True(tracked.Disposed);
            Assert.Equal(0, transfer.Charged);
            Assert.Equal(new long[] { 0, 1, 2, 3 }, opens.Calls);
        }

        [Fact]
        public async Task NothingIsOpenedAheadOnceIssuanceStops()
        {
            var opens = new ScriptedOpens();
            var asked = 0;
            await using var window = Window(opens, Sizes(6), depth: 4, mayIssue: () => ++asked <= 2);
            for (int i = 0; i < 6; i++)
            {
                opens.Open(i).SetResult(new MemoryStream());
            }

            using (var opened = await window.NextAsync())
            {
                Assert.NotNull(opened.Stream);
            }
            Assert.Equal(new long[] { 0, 1, 2 }, opens.Calls);
            for (int i = 1; i < 6; i++)
            {
                using var opened = await window.NextAsync();
                Assert.NotNull(opened.Stream);
                // A head goes out on its own take only
                Assert.Equal(Enumerable.Range(0, Math.Max(3, i + 1)).Select(x => (long)x), opens.Calls);
            }
            Assert.Equal(3, asked);
        }

        [Fact]
        public async Task AnObjectAdmittedAfterTheStopIsNeverOpened()
        {
            var opens = new ScriptedOpens();
            var transfer = new TransferAllowance(4);
            var open = true;
            await using var window = Window(opens, Sizes(3, 4), depth: 2, transfer, () => Volatile.Read(ref open));
            opens.Open(0).SetResult(new MemoryStream());

            var first = await window.NextAsync();
            Assert.NotNull(first.Stream);
            // Entry 1 waits behind 0's charge
            Assert.Equal(new long[] { 0 }, opens.Calls);
            Assert.Equal(4, transfer.Charged);

            Volatile.Write(ref open, false);
            first.Dispose();
            using (var second = await window.NextAsync())
            {
                Assert.True(second.Stopped);
                Assert.Null(second.Stream);
            }
            Assert.Equal(new long[] { 0 }, opens.Calls);
            Assert.Equal(0, transfer.Charged);
        }

        [Fact]
        public async Task AheadObjectsStayWithinTheWindowBytes()
        {
            const long mib = 1 << 20;
            var opens = new ScriptedOpens();
            var sizes = new[] { 6 * mib, 6 * mib, 20 * mib, 6 * mib, 6 * mib, 6 * mib };
            await using var window = Window(opens, sizes, depth: 8);
            for (int i = 0; i < sizes.Length; i++)
            {
                opens.Open(i).SetResult(new MemoryStream());
            }

            // Two 6 MiB objects, since a third would pass 16 MiB
            using (await window.NextAsync())
            {
                Assert.Equal(new long[] { 0, 1 }, opens.Calls);
            }
            // The 20 MiB object waits until it is the head
            using (await window.NextAsync())
            {
                Assert.Equal(new long[] { 0, 1 }, opens.Calls);
            }
            using (await window.NextAsync())
            {
                Assert.Equal(new long[] { 0, 1, 2 }, opens.Calls);
                Assert.Equal(0, window.Outstanding);
            }
            using (await window.NextAsync())
            {
                Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, opens.Calls);
                Assert.Equal(1, window.Outstanding);
            }
            using (await window.NextAsync())
            {
                Assert.Equal(new long[] { 0, 1, 2, 3, 4, 5 }, opens.Calls);
            }
            using (await window.NextAsync())
            {
            }
        }

        [Fact]
        public async Task AdmissionsKeepOrderAndNeverDeadlock()
        {
            var opens = new ScriptedOpens();
            var transfer = new TransferAllowance(10);
            var sizes = new long[] { 4, 4, 4, 20, 4, 4 };
            var streams = sizes.Select(_ => new MemoryStream()).ToArray();
            await using var window = Window(opens, sizes, depth: 4, transfer);
            for (int i = 0; i < sizes.Length; i++)
            {
                opens.Open(i).SetResult(streams[i]);
            }

            for (int i = 0; i < sizes.Length; i++)
            {
                var next = window.NextAsync().AsTask();
                await MaintenanceBucketsTests.Until(() => next.IsCompleted);
                using var opened = await next;
                Assert.Same(streams[i], opened.Stream);
                if (i == 3)
                {
                    // 4 and 5 queue behind the oversized object
                    Assert.Equal(20, transfer.Charged);
                }
            }
            // Every object opened once, in any call order
            Assert.Equal(Enumerable.Range(0, sizes.Length).Select(x => (long)x), opens.Calls.Order());
            Assert.Equal(0, transfer.Charged);
        }

        [Fact]
        public async Task AheadOpensOnlySpendSpareRequestTokens()
        {
            var clock = new FakeTimeProvider();
            // Ten tokens on hand, one of them the reserve ahead opens leave
            var buckets = new MaintenanceBuckets(clock, 200, 10, 1L << 30, 1L << 30);
            var opens = new ScriptedOpens();
            for (int i = 0; i < 40; i++)
            {
                opens.Open(i).SetResult(new MemoryStream());
            }
            await using var window = new CommitReadAhead(MeteredFileStorage.Wrap(opens, buckets, ListingPaging.Azure), index => ScriptedOpens.Entry(index), 40, 20, null, () => true, default);

            using (await window.NextAsync())
            {
                Assert.Equal(Enumerable.Range(0, 9).Select(x => (long)x), opens.Calls);
                Assert.Equal(0, buckets.Requests.QueueLength);
            }
            // Without spare tokens only each head goes out, on its own take
            for (int i = 1; i < 10; i++)
            {
                using (await window.NextAsync())
                {
                    Assert.Equal(Enumerable.Range(0, Math.Max(9, i + 1)).Select(x => (long)x), opens.Calls);
                    Assert.Equal(0, buckets.Requests.QueueLength);
                }
            }

            // A head waits for its token alone, and nothing queues behind it
            var head = window.NextAsync().AsTask();
            Assert.Equal(1, buckets.Requests.QueueLength);
            Assert.Equal(0, window.Outstanding);
            clock.Advance(TimeSpan.FromMilliseconds(5));
            using (await head)
            {
                Assert.Equal(Enumerable.Range(0, 11).Select(x => (long)x), opens.Calls);
            }

            // Another job's waiting request goes before the next head, and nothing goes ahead past it
            var foreign = buckets.Requests.AcquireAsync(1).AsTask();
            var next = window.NextAsync().AsTask();
            Assert.Equal(2, buckets.Requests.QueueLength);
            Assert.Equal(0, window.Outstanding);
            clock.Advance(TimeSpan.FromMilliseconds(5));
            await foreign;
            Assert.False(next.IsCompleted);
            clock.Advance(TimeSpan.FromMilliseconds(5));
            using (await next)
            {
                Assert.Equal(Enumerable.Range(0, 12).Select(x => (long)x), opens.Calls);
            }

            // A refilled bucket opens ahead again down to its reserve
            clock.Advance(TimeSpan.FromSeconds(1));
            using (await window.NextAsync())
            {
                Assert.Equal(Enumerable.Range(0, 21).Select(x => (long)x), opens.Calls);
                Assert.Equal(0, buckets.Requests.QueueLength);
            }
        }

        [Fact]
        public async Task NothingGoesAheadOfAnOpenWaitingForItsAdmission()
        {
            var opens = new ScriptedOpens();
            var transfer = new TransferAllowance(10);
            var streams = Enumerable.Range(0, 4).Select(_ => new MemoryStream()).ToArray();
            await using var window = Window(opens, Sizes(4, 6), depth: 4, transfer);
            for (int i = 0; i < 4; i++)
            {
                opens.Open(i).SetResult(streams[i]);
            }

            for (int i = 0; i < 4; i++)
            {
                var next = window.NextAsync().AsTask();
                await MaintenanceBucketsTests.Until(() => next.IsCompleted);
                using var opened = await next;
                Assert.Same(streams[i], opened.Stream);
                // The next object waits for this one's charge, and the window holds no more behind it
                Assert.True(window.Outstanding <= 1, $"{window.Outstanding} objects issued ahead of take {i}");
            }
            Assert.Equal(Enumerable.Range(0, 4).Select(x => (long)x), opens.Calls.Order());
            Assert.Equal(0, transfer.Charged);
        }

        [Fact]
        public async Task DepthOneOpensOnlyOnTake()
        {
            var opens = new ScriptedOpens();
            await using var window = Window(opens, Sizes(5), depth: 1);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(i, opens.Calls.Count);
                opens.Open(i).SetResult(new MemoryStream());
                using var opened = await window.NextAsync();
                Assert.Equal(Enumerable.Range(0, i + 1).Select(x => (long)x), opens.Calls);
            }
        }

        [Fact]
        public void DepthIsATenthOfASecondOfRequestsOnQualifiedStores()
        {
            var directory = StoreFixtures.TempDirectory(nameof(DepthIsATenthOfASecondOfRequestsOnQualifiedStores));
            using var local = Files.Of.LocalDisk(directory);
            using var azure = StoreFixtures.Azure();
            using var s3 = StoreFixtures.S3();
            using var memory = Files.Of.InternalMemory($"./depth-{Guid.NewGuid():N}");
            using var gcs = StoreFixtures.Gcs();
            using var wrapped = new ForwardingStorage(Files.Of.LocalDisk(directory));
            foreach (var qualified in new[] { local, azure })
            {
                var store = StoreDescriptor.Of(qualified);
                Assert.Equal(20, CommitReadAhead.Depth(store, 200));
                Assert.Equal(100, CommitReadAhead.Depth(store, 1_000));
                Assert.Equal(500, CommitReadAhead.Depth(store, 5_000));
                Assert.Equal(512, CommitReadAhead.Depth(store, 10_000));
                Assert.Equal(20, CommitReadAhead.Depth(store, 100));
            }
            foreach (var single in new[] { s3, memory, gcs, wrapped })
            {
                var store = StoreDescriptor.Of(single);
                Assert.All(new long[] { 100, 200, 1_000, 5_000, 10_000 }, rate => Assert.Equal(1, CommitReadAhead.Depth(store, rate)));
            }
            Assert.Equal(StoreKind.Unknown, StoreDescriptor.Of(wrapped).Kind);
        }
    }
}
