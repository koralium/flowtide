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

using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    [CollectionDefinition(nameof(RowStoreHostRaceCollection), DisableParallelization = true)]
    public class RowStoreHostRaceCollection
    {
    }

    [Collection(nameof(RowStoreHostRaceCollection))]
    public class RowStoreHostTests
    {
        // Key, a kept number, a stored text and a stored number
        private sealed record Row(long Key, long Kept, string Text, long Number, int Weight = 1);

        private sealed record Sent(int Weight, long Key, long Kept, ulong Reference);

        private static Task<RowStoreHost> Host(RowHeapTests.Stream stream, bool keepNumber = false, RowStoreMode mode = RowStoreMode.Keyed)
        {
            var layout = keepNumber ? new RowStoreHostLayout(4, [0], [1, 3], [2]) : new RowStoreHostLayout(4, [0], [1], [2, 3]);
            var options = new RowStoreOptions()
            {
                Mode = mode,
                Heap = new RowHeapOptions() { PageTargetBytes = 2000 }
            };
            return RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);
        }

        private static async Task<List<Sent>> Apply(RowStoreHost host, IReadOnlyList<Row> rows, Func<EventBatchData, int, bool>? filter = null)
        {
            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (var row in rows)
            {
                columns[0].Add(new Int64Value(row.Key));
                columns[1].Add(new Int64Value(row.Kept));
                columns[2].Add(new StringValue(row.Text));
                columns[3].Add(new Int64Value(row.Number));
                weights.Add(row.Weight);
            }
            using var result = await host.ApplyAsync(batch, weights, filter);
            var sent = new List<Sent>();
            for (int i = 0; i < result.InsertRows.Count; i++)
            {
                int row = result.InsertRows[i];
                sent.Add(new Sent(1, rows[row].Key, rows[row].Kept, (ulong)result.InsertReferences.GetValueAt(i, default).AsLong));
            }
            for (int i = 0; i < result.DeleteRows.Count; i++)
            {
                int row = result.DeleteRows[i];
                var kept = result.DeleteKeptColumns[0].GetValueAt(i, default);
                sent.Add(new Sent(-1, rows[row].Key, kept.Type == ArrowTypeId.Int64 ? kept.AsLong : 0, (ulong)result.DeleteReferences.GetValueAt(i, default).AsLong));
            }
            return sent;
        }

        private static async Task<Row?> Lookup(RowStoreHost host, long key)
        {
            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            columns[0].Add(new Int64Value(key));
            for (int i = 1; i < columns.Length; i++)
            {
                columns[i].Add(NullValue.Instance);
            }
            var found = await host.LookupAsync(new ColumnRowReference() { referenceBatch = batch, RowIndex = 0 });
            if (found == null)
            {
                return null;
            }
            try
            {
                return new Row(found[0].GetValueAt(0, default).AsLong, found[1].GetValueAt(0, default).AsLong, found[2].GetValueAt(0, default).AsString.ToString(), found[3].GetValueAt(0, default).AsLong);
            }
            finally
            {
                foreach (var column in found)
                {
                    column.Dispose();
                }
            }
        }

        private static async Task<long?> LookupNumber(RowStoreHost host, Row row)
        {
            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            columns[0].Add(new Int64Value(row.Key));
            columns[1].Add(new Int64Value(row.Kept));
            columns[2].Add(new StringValue(row.Text));
            columns[3].Add(NullValue.Instance);
            var found = await host.LookupAsync(new ColumnRowReference() { referenceBatch = batch, RowIndex = 0 });
            if (found == null)
            {
                return null;
            }
            long number = found[3].GetValueAt(0, default).AsLong;
            foreach (var column in found)
            {
                column.Dispose();
            }
            return number;
        }

        private static async Task<int> Leaves(RowStoreHost host)
        {
            int leaves = 0;
            using var iterator = host.CreateKeyIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                leaves++;
            }
            return leaves;
        }

        private static async Task Checkpoint(RowHeapTests.Stream stream, RowStoreHost host)
        {
            await host.BeginCheckpointAsync();
            await host.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
        }

        private static List<Row> Rows(int count, string text = "text")
        {
            var rows = new List<Row>();
            for (int i = 0; i < count; i++)
            {
                rows.Add(new Row(i, i * 10, text + i, i * 100));
            }
            return rows;
        }

        [Fact]
        public async Task NewRowsAreStoredAndSentWithTheirReferences()
        {
            using var stream = await Open();
            using var host = await Host(stream);

            var sent = await Apply(host, Rows(50));

            Assert.Equal(50, sent.Count);
            Assert.All(sent, x => Assert.Equal(1, x.Weight));
            Assert.Equal(50, sent.Select(x => x.Reference).Distinct().Count());
            Assert.Equal(50, host.Store.RowCount);
            Assert.Equal(new Row(7, 70, "text7", 700), await Lookup(host, 7));
            Assert.Null(await Lookup(host, 50));
        }

        [Fact]
        public async Task AnUnchangedBatchSendsNothingAndStoresNothing()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            await Apply(host, Rows(50));
            long sequences = host.Store.Heap.SequenceCount;

            var sent = await Apply(host, Rows(50));

            Assert.Empty(sent);
            Assert.Equal(50, host.Store.RowCount);
            Assert.Equal(sequences, host.Store.Heap.SequenceCount);
            Assert.Equal(50, host.VerifiedRows);
            Assert.Equal(1, host.VerifyReads);
            Assert.Equal(0, host.Store.WaitingRows);
        }

        [Fact]
        public async Task AChangedStoredColumnGetsANewVersionAndRetiresTheOld()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            var first = await Apply(host, Rows(10));
            ulong old = first.Single(x => x.Key == 3).Reference;

            long verified = host.VerifiedRows;
            var sent = await Apply(host, [new Row(3, 30, "changed", 300)]);

            Assert.Equal(verified, host.VerifiedRows);
            Assert.Equal(2, sent.Count);
            var added = sent.Single(x => x.Weight == 1);
            var removed = sent.Single(x => x.Weight == -1);
            Assert.Equal(old, removed.Reference);
            Assert.NotEqual(old, added.Reference);
            Assert.Equal(30, removed.Kept);
            Assert.Equal(1, host.Store.WaitingRows);
            Assert.True(host.Store.Heap.IsLive(old));
            Assert.Equal(new Row(3, 30, "changed", 300), await Lookup(host, 3));

            await Checkpoint(stream, host);

            Assert.False(host.Store.Heap.IsLive(old));
            Assert.True(host.Store.Heap.IsLive(added.Reference));
            Assert.Equal(10, host.Store.RowCount);
        }

        [Fact]
        public async Task AChangedKeptColumnKeepsTheReference()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            var first = await Apply(host, Rows(10));
            ulong reference = first.Single(x => x.Key == 3).Reference;
            long sequences = host.Store.Heap.SequenceCount;

            var sent = await Apply(host, [new Row(3, 31, "text3", 300)]);

            Assert.Equal(2, sent.Count);
            Assert.Equal(new Sent(1, 3, 31, reference), sent.Single(x => x.Weight == 1));
            Assert.Equal(new Sent(-1, 3, 30, reference), sent.Single(x => x.Weight == -1));
            Assert.Equal(sequences, host.Store.Heap.SequenceCount);
            Assert.Equal(0, host.Store.WaitingRows);
            Assert.Equal(new Row(3, 31, "text3", 300), await Lookup(host, 3));
        }

        [Fact]
        public async Task ADeleteSendsTheStoredReferenceAndRetiresTheRow()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            var first = await Apply(host, Rows(10));
            ulong reference = first.Single(x => x.Key == 4).Reference;
            long verified = host.VerifiedRows;

            var sent = await Apply(host, [new Row(4, 0, "", 0, -1), new Row(99, 0, "", 0, -1)]);

            Assert.Equal([new Sent(-1, 4, 40, reference)], sent);
            Assert.Equal(verified, host.VerifiedRows);
            Assert.Equal(1, host.Store.WaitingRows);
            Assert.Null(await Lookup(host, 4));

            await Checkpoint(stream, host);

            Assert.False(host.Store.Heap.IsLive(reference));
            Assert.Equal(9, host.Store.RowCount);
        }

        [Fact]
        public async Task AKeyThatComesTwiceInABatchEndsAsItsLastRowSays()
        {
            using var stream = await Open();
            using var host = await Host(stream);

            var sent = await Apply(host,
            [
                new Row(1, 10, "a", 1),
                new Row(2, 20, "b", 2),
                new Row(1, 10, "c", 1),
                new Row(2, 20, "b", 2, -1),
                new Row(1, 11, "c", 1)
            ]);

            Assert.Equal(new Row(1, 11, "c", 1), await Lookup(host, 1));
            Assert.Null(await Lookup(host, 2));
            Assert.Equal([new Sent(1, 1, 11, sent[0].Reference)], sent);
            Assert.Equal(1, host.Store.RowCount);
            Assert.Equal(0, host.Store.WaitingRows);

            var next = await Apply(host,
            [
                new Row(1, 11, "d", 1),
                new Row(3, 30, "e", 3),
                new Row(1, 12, "c", 1),
                new Row(3, 30, "e", 3, -1)
            ]);

            Assert.Equal([new Sent(1, 1, 12, sent[0].Reference), new Sent(-1, 1, 11, sent[0].Reference)], next);
            Assert.Equal(new Row(1, 12, "c", 1), await Lookup(host, 1));
            Assert.Null(await Lookup(host, 3));

            await Checkpoint(stream, host);

            Assert.Equal(1, host.Store.RowCount);
        }

        [Fact]
        public async Task ARowThatFailsTheFilterIsNotStored()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            Func<EventBatchData, int, bool> filter = (batch, row) => batch.Columns[3].GetValueAt(row, default).AsLong >= 0;

            var first = await Apply(host, [new Row(1, 10, "a", 1), new Row(2, 20, "b", -5)], filter);

            Assert.Equal([1L], first.Select(x => x.Key));
            Assert.Equal(1, host.Store.RowCount);

            var second = await Apply(host, [new Row(1, 10, "a", -1)], filter);

            Assert.Equal([new Sent(-1, 1, 10, first[0].Reference)], second);
            Assert.Null(await Lookup(host, 1));
            Assert.Equal(1, host.Store.WaitingRows);
        }

        [Fact]
        public async Task AHostRefusesAnotherLayoutThanItWasStoredWith()
        {
            using var stream = await Open();
            var host = await Host(stream);
            await Apply(host, Rows(5));
            await Checkpoint(stream, host);
            host.Dispose();

            await stream.Manager.InitializeAsync();
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => Host(stream, keepNumber: true));

            Assert.Equal(
                "Row store 'rows' is stored in mode Keyed with 4 columns, key columns [0], kept columns [1] and stored columns [2, 3] at output positions [], the stream asks for mode Keyed with 4 columns, [0], [1, 3] and [2] at [].",
                exception.Message);
        }

        [Fact]
        public async Task AnAppendOnlyHostStoresEveryRowAndTakesNoneBack()
        {
            using var stream = await Open();
            using var host = await Host(stream, mode: RowStoreMode.Append);
            Func<EventBatchData, int, bool> filter = (batch, row) => batch.Columns[3].GetValueAt(row, default).AsLong != 300;

            var first = await Apply(host, Rows(10), filter);
            var second = await Apply(host, Rows(10), filter);

            Assert.Equal(9, first.Count);
            Assert.DoesNotContain(first, x => x.Key == 3);
            Assert.Equal(18, first.Concat(second).Select(x => x.Reference).Distinct().Count());
            Assert.All(first.Concat(second), x => Assert.Equal(1, x.Weight));
            Assert.Equal(18, host.Store.RowCount);
            Assert.Equal(0, host.VerifiedRows);

            var lookup = await Assert.ThrowsAsync<InvalidOperationException>(() => Lookup(host, 1));
            Assert.Equal("Row store 'rows' is append-only, its rows are not known by key.", lookup.Message);

            await Checkpoint(stream, host);
            Assert.Equal(18, host.Store.RowCount);

            var negative = await Assert.ThrowsAsync<InvalidOperationException>(() => Apply(host, [new Row(1, 10, "text1", 100, -1)]));
            Assert.Equal("Row 0 of the batch has weight -1, row store 'rows' is append-only and takes no row back.", negative.Message);
            var failed = await Assert.ThrowsAsync<InvalidOperationException>(() => host.BeginCheckpointAsync());
            Assert.Equal("Row store 'rows' failed in an earlier call, it is opened anew from the last checkpoint.", failed.Message);
        }

        [Fact]
        public async Task RowsWithAnEqualHashAreComparedWithTheStoredRow()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            host.HashHookForTests = x => 42;
            var first = await Apply(host, Rows(20));

            var sent = await Apply(host, [new Row(3, 30, "other", 300), new Row(4, 40, "text4", 400), new Row(5, 50, "text5", 501), new Row(6, 61, "text6", 600)]);

            Assert.Equal(4, host.VerifiedRows);
            Assert.Equal([3L, 5L, 6L], sent.Where(x => x.Weight == 1).Select(x => x.Key).Order());
            Assert.Equal([3L, 5L, 6L], sent.Where(x => x.Weight == -1).Select(x => x.Key).Order());
            Assert.NotEqual(first.Single(x => x.Key == 3).Reference, sent.Single(x => x.Key == 3 && x.Weight == 1).Reference);
            Assert.NotEqual(first.Single(x => x.Key == 5).Reference, sent.Single(x => x.Key == 5 && x.Weight == 1).Reference);
            Assert.Equal(first.Single(x => x.Key == 6).Reference, sent.Single(x => x.Key == 6 && x.Weight == 1).Reference);
            Assert.Equal(new Row(3, 30, "other", 300), await Lookup(host, 3));
            Assert.Equal(new Row(5, 50, "text5", 501), await Lookup(host, 5));
            Assert.Equal(2, host.Store.WaitingRows);
        }

        [Fact]
        public async Task ALargeBatchWithScatteredChangesKeepsEveryRow()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            var rows = Rows(3000, new string('x', 60));
            await Apply(host, rows);
            await Checkpoint(stream, host);
            Assert.True(host.Store.Heap.SequenceCount > 20);

            var random = new Random(123);
            for (int i = 0; i < rows.Count; i++)
            {
                int pick = random.Next(10);
                if (pick == 0)
                {
                    rows[i] = rows[i] with { Text = "changed" + i };
                }
                else if (pick == 1)
                {
                    rows[i] = rows[i] with { Kept = rows[i].Kept + 1 };
                }
                else if (pick == 2)
                {
                    rows[i] = rows[i] with { Weight = -1 };
                }
            }
            var sent = await Apply(host, rows);
            await Checkpoint(stream, host);

            int payload = rows.Count(x => x.Text.StartsWith("changed"));
            int kept = rows.Count(x => x.Weight == 1 && x.Kept != x.Key * 10 && !x.Text.StartsWith("changed"));
            int deleted = rows.Count(x => x.Weight == -1);
            Assert.Equal(payload + kept, sent.Count(x => x.Weight == 1));
            Assert.Equal(payload + kept + deleted, sent.Count(x => x.Weight == -1));
            Assert.Equal(rows.Count - deleted, host.Store.RowCount);
            foreach (var row in rows)
            {
                Assert.Equal(row.Weight == -1 ? null : row, await Lookup(host, row.Key));
            }
        }

        [Fact]
        public async Task ACheckpointWaitsForTheBatchThatIsBeingApplied()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            await Apply(host, Rows(200));
            await Checkpoint(stream, host);
            using var entered = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            Func<EventBatchData, int, bool> filter = (batch, row) =>
            {
                if (row == 100)
                {
                    entered.Set();
                    Assert.True(release.Wait(Limit));
                }
                return true;
            };

            var apply = Task.Run(() => Apply(host, Rows(200, "next"), filter));
            Assert.True(entered.Wait(Limit));
            var begin = Task.Run(() => host.BeginCheckpointAsync());
            var lookup = Task.Run(() => Lookup(host, 5));
            var watch = System.Diagnostics.Stopwatch.StartNew();
            while (host.WaitersForTests < 2)
            {
                Assert.True(watch.Elapsed < Limit);
                await Task.Yield();
            }

            Assert.False(begin.IsCompleted);
            Assert.False(lookup.IsCompleted);
            release.Set();
            var sent = await apply.WaitAsync(Limit);
            await Task.WhenAll(begin, lookup).WaitAsync(Limit);
            await host.CheckpointAsync();
            await stream.Manager.CheckpointAsync();

            Assert.Equal(400, sent.Count);
            Assert.Equal(200, host.Store.RowCount - host.Store.WaitingRows);
            Assert.Equal(new Row(199, 1990, "next199", 19900), await Lookup(host, 199));
        }

        [Fact]
        public async Task RowsAppliedAfterTheBarrierAreLeftOutOfItsCheckpoint()
        {
            var rows = Rows(300, new string('b', 30));
            using var first = await Open();
            using (var host = await Host(first))
            {
                await Apply(host, rows);
                await host.BeginCheckpointAsync();

                var later = new List<Row>() { rows[5] with { Text = "after the barrier" }, rows[6] with { Kept = 7 }, rows[7] with { Weight = -1 }, new Row(900, 9000, "new", 90000) };
                Assert.Equal(6, (await Apply(host, later)).Count);

                await host.CheckpointAsync();
                await first.Manager.CheckpointAsync();
            }

            using var second = await Open(first.Provider);
            using var restored = await Host(second);

            Assert.Empty(await Apply(restored, rows));
            Assert.Equal(300, restored.Store.RowCount);
            Assert.Equal(0, restored.Store.WaitingRows);
            Assert.Null(await Lookup(restored, 900));
            Assert.Equal(rows[5], await Lookup(restored, 5));
            Assert.Equal(rows[7], await Lookup(restored, 7));
        }

        [Fact]
        public async Task UnchangedRowsAreVerifiedOnMergedPages()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            var rows = Rows(2000, new string('y', 40));
            await Apply(host, rows);
            await Checkpoint(stream, host);
            for (int round = 0; round < 6; round++)
            {
                for (int i = round; i < rows.Count; i += 3)
                {
                    rows[i] = rows[i] with { Text = "round" + round + "row" + i };
                }
                await Apply(host, rows);
                await Checkpoint(stream, host);
            }
            Assert.True(host.Store.Heap.PagesMerged > 0);
            long verified = host.VerifiedRows;

            var sent = await Apply(host, rows);

            Assert.Empty(sent);
            Assert.Equal(verified + rows.Count, host.VerifiedRows);
            Assert.Equal(rows.Count, host.Store.RowCount);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task KeysInAnyColumnAndOfAnyShapeAreSortedAndFound(int shape)
        {
            using var stream = await Open();
            var layout = shape == 0 ? new RowStoreHostLayout(4, [2, 0], [1], [3])
                : shape == 1 ? new RowStoreHostLayout(4, [2], [1, 0], [3])
                : shape == 2 ? new RowStoreHostLayout(4, [1, 0], [2], [3])
                : new RowStoreHostLayout(4, [1], [0, 2], [3]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);
            var random = new Random(123);
            var rows = new List<Row>();
            for (int i = 0; i < 1500; i++)
            {
                rows.Add(new Row(i % 50, i, "name" + random.Next(1000000) + "-" + i, i * 3));
            }

            var first = await Apply(host, rows);
            Assert.Equal(1500, first.Count);
            Assert.Equal(2, await Leaves(host));
            Assert.Empty(await Apply(host, rows));
            Assert.Equal(rows[700].Number, await LookupNumber(host, rows[700]));
            Assert.Null(await LookupNumber(host, rows[700] with { Key = 999, Kept = 99999, Text = "none" }));
            Assert.Empty(await Apply(host, rows.OrderBy(x => x.Text, StringComparer.Ordinal).ToList()));

            for (int i = 0; i < rows.Count; i += 7)
            {
                rows[i] = rows[i] with { Number = -rows[i].Number - 1 };
            }
            var second = await Apply(host, rows);

            int changed = (rows.Count + 6) / 7;
            Assert.Equal(changed, second.Count(x => x.Weight == 1));
            Assert.Equal(changed, second.Count(x => x.Weight == -1));
            Assert.Equal(1500, host.Store.RowCount - host.Store.WaitingRows);
        }

        [Fact]
        public async Task RowsAreFoundAgainWhenTheTreeIsReadFromStorage()
        {
            var rows = Rows(1200, new string('z', 30));
            var left = rows.Where(x => x.Key % 4 != 0).ToList();
            using var first = await Open();
            using (var host = await Host(first))
            {
                await Apply(host, rows);
                await Checkpoint(first, host);
            }

            using var second = await Open(first.Provider);
            using (var host = await Host(second))
            {
                Assert.Empty(await Apply(host, rows));
                var deleted = rows.Where(x => x.Key % 4 == 0).Select(x => x with { Weight = -1 }).ToList();
                Assert.Equal(deleted.Count, (await Apply(host, deleted)).Count);
                await Checkpoint(second, host);
            }

            using var third = await Open(first.Provider);
            using var last = await Host(third);
            Assert.Empty(await Apply(last, left));
            Assert.Equal(left.Count, last.Store.RowCount);
            Assert.Null(await Lookup(last, 400));
            Assert.Equal(rows[401], await Lookup(last, 401));
        }

        [Fact]
        public async Task RowByRowWritesSplitLeavesThatSortedWritesFilled()
        {
            using var stream = await Open();
            var layout = new RowStoreHostLayout(4, [0], [1, 2], [3]);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);
            var rows = new List<Row>();
            for (int i = 0; i < 1500; i++)
            {
                rows.Add(new Row(i * 2, i, new string('k', 80) + i, i));
            }
            await Apply(host, rows);
            await Apply(host, rows.Where(x => x.Key % 10 == 0).Select(x => x with { Weight = -1 }).ToList());
            rows.RemoveAll(x => x.Key % 10 == 0);
            int leaves = await Leaves(host);

            var more = new List<Row>();
            for (int i = 0; i < 1500; i++)
            {
                more.Add(new Row(i * 2 + 1, i, new string('m', 80) + i, i));
            }
            more.Add(more[0] with { Kept = 7 });
            await Apply(host, more);
            rows.AddRange(more.Skip(1));

            Assert.True(await Leaves(host) > leaves);
            Assert.Empty(await Apply(host, rows));
            Assert.Equal(rows.Count, host.Store.RowCount - host.Store.WaitingRows);
            Assert.Equal(7, (await Lookup(host, 1))!.Kept);
            Assert.Null(await Lookup(host, 20));
        }

        private sealed class RowCollector : IRowHeapVisitor
        {
            public List<(int position, long number)> Rows { get; } = new List<(int position, long number)>();

            public int Pages { get; private set; }

            public void VisitRows(EventBatchData page, int[] pageRows, int first, int length)
            {
                Pages++;
                for (int j = 0; j < length; j++)
                {
                    Rows.Add((first + j, page.Columns[1].GetValueAt(pageRows[j], default).AsLong));
                }
            }
        }

        [Fact]
        public async Task AVisitShowsEachReferencedRowOnceAndRefusesANullReference()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            var sent = await Apply(host, Rows(600, new string('v', 30)));
            using var reader = host.Store.CreateReader();
            using var references = Column.Create(GlobalMemoryManager.Instance);
            var asked = new List<Sent>();
            for (int i = 599; i >= 0; i -= 7)
            {
                var row = sent.Single(x => x.Key == i);
                asked.Add(row);
                references.Add(new RowRefValue(row.Reference));
            }
            var order = new int[asked.Count];
            var collector = new RowCollector();

            await reader.VisitAsync(references, asked.Count, order, collector);

            Assert.True(collector.Pages > 3);
            Assert.Equal(asked.Count, collector.Rows.Count);
            Assert.Equal(Enumerable.Range(0, asked.Count), collector.Rows.Select(x => x.position));
            foreach (var (position, number) in collector.Rows)
            {
                Assert.Equal(asked[order[position]].Key * 100, number);
            }

            references.Add(NullValue.Instance);
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.VisitAsync(references, asked.Count + 1, new int[asked.Count + 1], collector));
            Assert.Equal("Row heap 'rows' got 1 null references to visit, a visit needs a row for each.", exception.Message);
        }

        private static EventBatchData Keys(params long[] keys)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            foreach (var key in keys)
            {
                column.Add(new Int64Value(key));
            }
            return new EventBatchData(new IColumn[] { column });
        }

        [Fact]
        public async Task RemovingKeysInBulkRetiresTheirRowsAndSkipsMissingKeys()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            var sent = await Apply(host, Rows(50));
            using var keys = Keys(3, 7, 60, 61);
            using var result = host.CreateResult();

            await host.RemoveAsync(keys, 4, result);

            Assert.Equal([0, 1], Enumerable.Range(0, result.DeleteRows.Count).Select(x => result.DeleteRows[x]));
            Assert.Equal(sent.Single(x => x.Key == 7).Reference, (ulong)result.DeleteReferences.GetValueAt(1, default).AsLong);
            Assert.Equal(70, result.DeleteKeptColumns[0].GetValueAt(1, default).AsLong);
            Assert.Equal(2, host.Store.WaitingRows);
            Assert.Null(await Lookup(host, 3));

            await Checkpoint(stream, host);

            Assert.False(host.Store.Heap.IsLive(sent.Single(x => x.Key == 3).Reference));
            Assert.Equal(48, host.Store.RowCount);
        }

        [Fact]
        public async Task ManyKeysAreLookedUpInOneCallEachWithItsOwnRow()
        {
            using var stream = await Open();
            using var host = await Host(stream);
            await Apply(host, Rows(900, new string('l', 40)));
            await Apply(host, [new Row(5, 50, "newer", 500), new Row(350, 3500, "newest", 35000)]);
            using var keys = Keys(700, 5, 9999, 350, 5, 12);

            var (rows, found) = await host.LookupManyAsync(keys, 6);

            Assert.NotNull(rows);
            try
            {
                Assert.Equal(4, found);
                var asked = host.LookupRows(found).ToArray();
                Assert.Equal([0, 3, 4, 5], asked.Order());
                long[] wanted = [700, 5, 9999, 350, 5, 12];
                for (int i = 0; i < found; i++)
                {
                    long key = wanted[asked[i]];
                    Assert.Equal(key, rows[0].GetValueAt(i, default).AsLong);
                    Assert.Equal(key * 10, rows[1].GetValueAt(i, default).AsLong);
                    string text = key == 5 ? "newer" : key == 350 ? "newest" : new string('l', 40) + key;
                    Assert.Equal(text, rows[2].GetValueAt(i, default).AsString.ToString());
                    Assert.Equal(key * 100, rows[3].GetValueAt(i, default).AsLong);
                }
            }
            finally
            {
                foreach (var column in rows)
                {
                    column.Dispose();
                }
            }
        }

        [Fact]
        public async Task AColumnSentTwiceIsStoredOnceAndStillSentWhereItIsKept()
        {
            using var stream = await Open();
            var emit = new List<int>() { 0, 2, 2, 1 };
            var layout = RowStoreHost.ReadLayout(new RowStoreDefinition() { Name = "rows", StoredColumns = [1] }, 4, [0], emit);
            Assert.NotNull(layout);
            Assert.Equal([2, 1], layout.KeptColumns);
            Assert.Equal([2], layout.DeferredColumns);
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 2000 } };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);
            var columns = new IColumn[4];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            columns[0].Add(new Int64Value(1));
            columns[1].Add(new Int64Value(10));
            columns[2].Add(new StringValue("text"));
            columns[3].Add(new Int64Value(100));
            weights.Add(1);

            var result = await host.ApplyAsync(batch, weights, null);
            var (inserts, deletes) = host.CreateBatches(batch, result, emit);

            Assert.Null(deletes);
            Assert.NotNull(inserts);
            try
            {
                var sent = inserts.EventBatchData.Columns;
                Assert.Equal(4, sent.Count);
                Assert.Equal(1, sent[0].GetValueAt(0, default).AsLong);
                Assert.Equal("text", sent[1].GetValueAt(0, default).AsString.ToString());
                Assert.Equal(10, sent[2].GetValueAt(0, default).AsLong);
                Assert.Equal(ArrowTypeId.RowRef, sent[3].GetValueAt(0, default).Type);
            }
            finally
            {
                inserts.Rent(1);
                inserts.Return();
            }
        }

        [Fact]
        public async Task AKeySetHoldsEachKeyOnce()
        {
            using var stream = await Open();
            var set = await RowStoreKeySet.CreateAsync(stream.Client, "keyset", 1, GlobalMemoryManager.Instance);
            using var keys = Keys(5, 3, 5, 9, 1);

            await set.AddAsync(keys, 5);
            using var more = Keys(2, 3);
            await set.AddAsync(more, 2);
            await set.AddAsync(more, 0);

            var stored = new List<long>();
            using (var iterator = set.CreateIterator())
            {
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    foreach (var kv in page)
                    {
                        stored.Add(kv.Key.referenceBatch.Columns[0].GetValueAt(kv.Key.RowIndex, default).AsLong);
                    }
                }
            }
            Assert.Equal([1L, 2L, 3L, 5L, 9L], stored);

            await set.Clear();
        }

        [Fact]
        public void ABulkInsertFromAnAlwaysNullColumnStoresNulls()
        {
            var columns = new List<int>() { 0 };
            using var storage = new FlowtideDotNet.Core.Operators.Normalization.NormalizeValueStorage(columns, GlobalMemoryManager.Instance);
            using var first = Keys(10, 20);
            var source = new EventBatchData(new IColumn[] { new AlwaysNullColumn() });
            var values = new ColumnRowReference[3];
            values[0] = new ColumnRowReference() { referenceBatch = first, RowIndex = 0 };
            values[1] = new ColumnRowReference() { referenceBatch = source, RowIndex = 0 };
            values[2] = new ColumnRowReference() { referenceBatch = source, RowIndex = 1 };

            storage.InsertFrom(values, [0], [0]);
            storage.InsertFrom(values, [1, 2], [1, 1]);

            Assert.Equal(3, storage.Count);
            var stored = storage.Get(0).referenceBatch.Columns[0];
            Assert.Equal(10, stored.GetValueAt(0, default).AsLong);
            Assert.Equal(ArrowTypeId.Null, stored.GetValueAt(1, default).Type);
            Assert.Equal(ArrowTypeId.Null, stored.GetValueAt(2, default).Type);
        }

        [Fact]
        public void TheValueStorageCountsItsRowsThroughBulkInsertsAndDeletes()
        {
            var columns = new List<int>() { 0 };
            using var storage = new FlowtideDotNet.Core.Operators.Normalization.NormalizeValueStorage(columns, GlobalMemoryManager.Instance);
            using var source = Keys(10, 20, 30, 40);
            var values = new ColumnRowReference[4];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = new ColumnRowReference() { referenceBatch = source, RowIndex = i };
            }

            storage.InsertFrom(values, [0, 1, 2, 3], [0, 0, 0, 0]);
            Assert.Equal(4, storage.Count);
            storage.DeleteBatch([1, 2]);

            Assert.Equal(2, storage.Count);
            Assert.Equal(10, storage.Get(0).referenceBatch.Columns[0].GetValueAt(0, default).AsLong);
            Assert.Equal(40, storage.Get(1).referenceBatch.Columns[0].GetValueAt(1, default).AsLong);
        }

        private sealed class ThrowingVisitor : IRowHeapVisitor
        {
            public void VisitRows(EventBatchData page, int[] pageRows, int first, int length)
            {
                throw new InvalidOperationException("the visitor failed");
            }
        }

        [Fact]
        public async Task AVisitRunsTheHookOnceAndHoldsNoPageAfterAFailingVisitor()
        {
            using var stream = await Open();
            int resolved = 0;
            var layout = new RowStoreHostLayout(4, [0], [1], [2, 3]);
            var options = new RowStoreOptions()
            {
                Mode = RowStoreMode.Keyed,
                Heap = new RowHeapOptions() { PageTargetBytes = 2000, PagesResolvedHookForTests = () => resolved++ }
            };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, options, GlobalMemoryManager.Instance);
            var sent = await Apply(host, Rows(300, new string('h', 30)));
            using var reader = host.Store.CreateReader();
            using var references = Column.Create(GlobalMemoryManager.Instance);
            foreach (var row in sent.Where(x => x.Key % 50 == 0))
            {
                references.Add(new RowRefValue(row.Reference));
            }
            var order = new int[references.Count];
            int before = resolved;

            await reader.VisitAsync(references, references.Count, order, new RowCollector());
            Assert.Equal(before + 1, resolved);

            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.VisitAsync(references, references.Count, order, new ThrowingVisitor()));
            Assert.Equal("the visitor failed", error.Message);
            Assert.Equal(0, reader.HeldPages);
        }

        [Fact]
        public void ARelationWithoutARowStoreHasNoHostLayout()
        {
            Assert.Null(RowStoreHost.ReadLayout(null, 3, [0], [0, 1, 2]));
            var layout = RowStoreHost.ReadLayout(new RowStoreDefinition() { Name = "rows", StoredColumns = [2] }, 3, [0], [0, 1, 2]);

            Assert.NotNull(layout);
            Assert.Equal([1], layout.KeptColumns);
            Assert.Equal([2], layout.DeferredColumns);
        }

        [Fact]
        public async Task ARestoredHostKnowsItsRows()
        {
            using var stream = await Open();
            var host = await Host(stream);
            var first = await Apply(host, Rows(200));
            await Checkpoint(stream, host);
            await Apply(host, [new Row(5, 50, "lost", 500)]);
            host.Dispose();

            await stream.Manager.InitializeAsync();
            using var restored = await Host(stream);

            Assert.Equal(new Row(5, 50, "text5", 500), await Lookup(restored, 5));
            Assert.Empty(await Apply(restored, Rows(200)));
            var sent = await Apply(restored, [new Row(5, 50, "new", 500)]);
            Assert.Equal(first.Single(x => x.Key == 5).Reference, sent.Single(x => x.Weight == -1).Reference);
        }
    }
}
