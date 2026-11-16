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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Threading.Tasks.Dataflow;
using Host = FlowtideDotNet.Core.Tests.MultiJoin.MultiJoinAllocationTests.Host;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinRoundTests
    {
        private static readonly string[] Plans = new[] { "chain", "star", "checkChain", "checkEqualOtherInput", "checkStar", "checkComparedTwice" };

        public static IEnumerable<object[]> Cases()
        {
            foreach (var plan in Plans)
            {
                for (int seed = 1; seed <= 4; seed++)
                {
                    // A row budget of one makes every lookup come in several parts, so a batch keeps its trees while it comes back to them
                    yield return new object[] { plan, 1024, seed };
                    yield return new object[] { plan, 1, seed };
                }
            }
        }

        private static StreamEventBatch Batch(long[][] rows, int[] weights)
        {
            var allocator = GlobalMemoryManager.Instance;
            int columnCount = rows[0].Length;
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                var column = Column.Create(allocator);
                for (int r = 0; r < rows.Length; r++)
                {
                    column.Add(new Int64Value(rows[r][c]));
                }
                columns[c] = column;
            }
            var weightList = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            for (int r = 0; r < rows.Length; r++)
            {
                weightList.Add(weights[r]);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weightList, iterations, new EventBatchData(columns)));
            batch.Rent(1);
            return batch;
        }

        private static void AddRows(Dictionary<string, int> rows, StreamEventBatch batch)
        {
            var data = batch.Data;
            var columns = data.EventBatchData.Columns;
            for (int r = 0; r < data.Weights.Count; r++)
            {
                var row = string.Join("|", columns.Select(c =>
                {
                    var value = c.GetValueAt(r, default);
                    return value.IsNull ? "null" : value.Type == ArrowTypeId.Boolean ? value.AsBool.ToString() : value.AsLong.ToString();
                }));
                int weight = rows.GetValueOrDefault(row) + data.Weights[r];
                if (weight == 0)
                {
                    rows.Remove(row);
                }
                else
                {
                    rows[row] = weight;
                }
            }
        }

        private static async Task<(MultiJoinOperator op, Host host)> Create(string planName)
        {
            var host = new Host();
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan(planName), 9);
            Assert.NotNull(plan);
            var op = new MultiJoinOperator(plan, host.FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            op.LinkTo(new BufferBlock<IStreamEvent>());
            await host.InitializeOperator(op);
            host.Operator = op;
            return (op, host);
        }

        // Reads what a call gives as the vertex does: every part is rented before the next one is asked for
        private static async Task Collect(IAsyncEnumerable<StreamEventBatch> output, Dictionary<string, int> rows)
        {
            await foreach (var part in output)
            {
                part.Rent(1);
                AddRows(rows, part);
                part.Return();
            }
        }

        [Theory]
        [MemberData(nameof(Cases))]
        public async Task ARoundGivesTheRowsOfItsBatchesTakenOneByOne(string planName, int rowBudget, int seed)
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int partsBefore = MultiJoinOperator.RoundPartBudgets;
            MultiJoinOperator.RowBudget = rowBudget;
            MultiJoinOperator.RoundPartBudgets = rowBudget == 1 ? 2 : partsBefore;
            // Every other seed looks the later joins of a batch up beside the one before, as when a lookup waits for a page
            bool besideBefore = MultiJoinOperator.ParallelLookupsAlways;
            MultiJoinOperator.ParallelLookupsAlways = seed % 2 == 0;
            var hosts = new List<Host>();
            try
            {
                var (oneByOne, firstHost) = await Create(planName);
                hosts.Add(firstHost);
                var (inRounds, secondHost) = await Create(planName);
                hosts.Add(secondHost);
                var columnCounts = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan(planName), 9)!.Inputs.Select(x => x.ColumnCount).ToArray();
                var random = new Random(seed * 7919 + rowBudget);
                var expected = new Dictionary<string, int>();
                var actual = new Dictionary<string, int>();
                bool anyRound = false;
                for (int round = 1; round <= 14; round++)
                {
                    var messages = new List<RoundMessage<StreamEventBatch>>();
                    var copies = new List<(int input, StreamEventBatch batch)>();
                    for (int input = 0; input < columnCounts.Length; input++)
                    {
                        if (random.Next(5) == 0)
                        {
                            continue;
                        }
                        // Few values, so the batches of a round meet each other's rows and the rows stored before
                        int count = random.Next(1, 13);
                        var rows = new long[count][];
                        var weights = new int[count];
                        for (int r = 0; r < count; r++)
                        {
                            rows[r] = Enumerable.Range(0, columnCounts[input]).Select(_ => (long)random.Next(4)).ToArray();
                            weights[r] = random.Next(4) == 0 ? -1 : 1;
                        }
                        messages.Add(new RoundMessage<StreamEventBatch>(input, Batch(rows, weights)));
                        copies.Add((input, Batch(rows, weights)));
                    }
                    if (messages.Count == 0)
                    {
                        continue;
                    }
                    anyRound |= messages.Count > 1;
                    foreach (var (input, batch) in copies)
                    {
                        await Collect(oneByOne.OnRecieve(input, batch, 0), expected);
                        batch.Return();
                    }
                    await Collect(messages.Count == 1 ? inRounds.OnRecieve(messages[0].TargetId, messages[0].Message, 0) : inRounds.OnRecieveRound(messages, 0), actual);
                    foreach (var message in messages)
                    {
                        message.Message.Return();
                    }
                    Assert.True(expected.Count == actual.Count && expected.All(x => actual.GetValueOrDefault(x.Key) == x.Value),
                        $"Round {round}: {string.Join("; ", expected.Keys.Union(actual.Keys).Where(x => expected.GetValueOrDefault(x) != actual.GetValueOrDefault(x)).Take(5).Select(x => $"{x}: one by one {expected.GetValueOrDefault(x)}, in a round {actual.GetValueOrDefault(x)}"))}");
                }
                Assert.True(anyRound, "No round had two batches");
                Assert.True(inRounds.RoundsSideBySide > 0, "No round had its batches work side by side");
                Assert.NotEmpty(expected);
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinOperator.RoundPartBudgets = partsBefore;
                MultiJoinOperator.ParallelLookupsAlways = besideBefore;
                await MultiJoinAllocationTests.DisposeAll(hosts);
            }
        }

        [Fact]
        public async Task DisposingRoundEnumeratorReturnsThePartAlreadyHandedOut()
        {
            var hosts = new List<Host>();
            var messages = new List<RoundMessage<StreamEventBatch>>();
            try
            {
                var (op, host) = await Create("chain");
                hosts.Add(host);
                var ignored = new Dictionary<string, int>();
                var seedRows = new[] { new long[] { 1, 10 }, new long[] { 1, 1, 20 }, new long[] { 1, 30 } };
                for (int input = 0; input < seedRows.Length; input++)
                {
                    var seed = Batch(new[] { seedRows[input] }, new[] { 1 });
                    try
                    {
                        await Collect(op.OnRecieve(input, seed, 0), ignored);
                    }
                    finally
                    {
                        seed.Return();
                    }
                }
                messages.Add(new RoundMessage<StreamEventBatch>(0, Batch(new[] { new long[] { 1, 11 } }, new[] { 1 })));
                messages.Add(new RoundMessage<StreamEventBatch>(1, Batch(new[] { new long[] { 1, 1, 21 } }, new[] { 1 })));
                PrimitiveList<int> weights;
                await using (var enumerator = op.OnRecieveRound(messages, 0).GetAsyncEnumerator())
                {
                    Assert.True(await enumerator.MoveNextAsync());
                    var part = enumerator.Current;
                    part.Rent(1);
                    weights = part.Data.Weights;
                    part.Return();
                }
                var rentCounter = typeof(PrimitiveList<int>).GetField("_rentCounter", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
                Assert.Equal(0, (int)rentCounter.GetValue(weights)!);
            }
            finally
            {
                foreach (var message in messages)
                {
                    message.Message.Return();
                }
                await MultiJoinAllocationTests.DisposeAll(hosts);
            }
        }

        private sealed class HoldSecondRoundScheduler : TaskScheduler
        {
            private int _queued;
            private Task? _second;

            protected override IEnumerable<Task>? GetScheduledTasks() => Array.Empty<Task>();

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

            protected override void QueueTask(Task task)
            {
                if (Interlocked.Increment(ref _queued) == 2)
                {
                    _second = task;
                    return;
                }
                ThreadPool.QueueUserWorkItem(_ => TryExecuteTask(task));
            }

            internal void RunSecond()
            {
                var task = Interlocked.Exchange(ref _second, null);
                if (task != null)
                {
                    TryExecuteTask(task);
                }
            }
        }

        [Fact]
        public async Task DisposingRoundEnumeratorReportsAStoreFailureThatHasNotBeenObserved()
        {
            var hosts = new List<Host>();
            var messages = new List<RoundMessage<StreamEventBatch>>();
            var hookBefore = MultiJoinOperator.RoundStoreHookForTests;
            var scheduler = new HoldSecondRoundScheduler();
            IAsyncEnumerator<StreamEventBatch>? enumerator = null;
            Task? disposing = null;
            try
            {
                var (op, host) = await Create("chain");
                hosts.Add(host);
                var ignored = new Dictionary<string, int>();
                var seedRows = new[] { new long[] { 1, 10 }, new long[] { 1, 1, 20 }, new long[] { 1, 30 } };
                for (int input = 0; input < seedRows.Length; input++)
                {
                    var seed = Batch(new[] { seedRows[input] }, new[] { 1 });
                    try
                    {
                        await Collect(op.OnRecieve(input, seed, 0), ignored);
                    }
                    finally
                    {
                        seed.Return();
                    }
                }
                messages.Add(new RoundMessage<StreamEventBatch>(0, Batch(new[] { new long[] { 1, 11 } }, new[] { 1 })));
                messages.Add(new RoundMessage<StreamEventBatch>(1, Batch(new[] { new long[] { 1, 1, 21 } }, new[] { 1 })));
                typeof(MultiJoinOperator).GetField("_scheduler", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.SetValue(op, scheduler);
                var failure = new IOException("Injected store failure after a successful round yield");
                MultiJoinOperator.RoundStoreHookForTests = input =>
                {
                    if (input == 0)
                    {
                        throw failure;
                    }
                };
                enumerator = op.OnRecieveRound(messages, 0).GetAsyncEnumerator();
                Assert.True(await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));
                enumerator.Current.Rent(1);
                enumerator.Current.Return();
                disposing = enumerator.DisposeAsync().AsTask();
                Assert.False(disposing.IsCompleted, "Disposal must await the scheduled store before returning its batch");
                scheduler.RunSecond();

                var thrown = await Assert.ThrowsAsync<IOException>(() => disposing.WaitAsync(TimeSpan.FromSeconds(30)));
                Assert.Same(failure, thrown);
            }
            finally
            {
                MultiJoinOperator.RoundStoreHookForTests = hookBefore;
                scheduler.RunSecond();
                if (disposing != null)
                {
                    // Its failure is what the test asserts, here it only has to have ended
                    await disposing.ContinueWith(static x => _ = x.Exception, TaskScheduler.Default).WaitAsync(TimeSpan.FromSeconds(30));
                }
                else if (enumerator != null)
                {
                    try
                    {
                        await enumerator.DisposeAsync();
                    }
                    catch (IOException)
                    {
                    }
                }
                foreach (var message in messages)
                {
                    message.Message.Return();
                }
                await MultiJoinAllocationTests.DisposeAll(hosts);
            }
        }

        private sealed class HoldFirstRejectSecondRoundScheduler : TaskScheduler
        {
            private int _queued;
            internal Task? First;

            protected override IEnumerable<Task>? GetScheduledTasks() => Array.Empty<Task>();

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

            protected override void QueueTask(Task task)
            {
                int queued = Interlocked.Increment(ref _queued);
                if (queued == 1)
                {
                    First = task;
                    return;
                }
                if (queued == 2)
                {
                    throw new InvalidOperationException("Rejected the second round task");
                }
                TryExecuteTask(task);
            }

            internal void RunFirst() => TryExecuteTask(First!);
        }

        [Fact]
        public async Task RoundSchedulerRejectionWaitsForTheReaderAlreadyStarted()
        {
            var hosts = new List<Host>();
            var messages = new List<RoundMessage<StreamEventBatch>>();
            var scheduler = new HoldFirstRejectSecondRoundScheduler();
            try
            {
                var (op, host) = await Create("chain");
                hosts.Add(host);
                typeof(MultiJoinOperator).GetField("_scheduler", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.SetValue(op, scheduler);
                messages.Add(new RoundMessage<StreamEventBatch>(0, Batch(new[] { new long[] { 1, 11 } }, new[] { 1 })));
                messages.Add(new RoundMessage<StreamEventBatch>(1, Batch(new[] { new long[] { 1, 1, 21 } }, new[] { 1 })));
                await using var enumerator = op.OnRecieveRound(messages, 0).GetAsyncEnumerator();
                Task<bool> moving = enumerator.MoveNextAsync().AsTask();
                try
                {
                    Assert.NotNull(scheduler.First);
                    Assert.False(moving.IsCompleted, "The round must retain its input until the reader already queued has ended.");
                }
                finally
                {
                    scheduler.RunFirst();
                    await ((Task<Task>)scheduler.First!).Unwrap().WaitAsync(TimeSpan.FromSeconds(30));
                    await Assert.ThrowsAsync<TaskSchedulerException>(() => moving);
                }
            }
            finally
            {
                foreach (var message in messages)
                {
                    message.Message.Return();
                }
                await MultiJoinAllocationTests.DisposeAll(hosts);
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task ABatchThatFailsInARoundFailsTheRound(bool inItsRead, bool canceled)
        {
            var hosts = new List<Host>();
            var hookBefore = MultiJoinOperator.RoundStoreHookForTests;
            var readHookBefore = MultiJoinOperator.RoundReadHookForTests;
            try
            {
                var (op, host) = await Create("star");
                hosts.Add(host);
                var columnCounts = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan("star"), 9)!.Inputs.Select(x => x.ColumnCount).ToArray();
                var messages = new List<RoundMessage<StreamEventBatch>>();
                for (int input = 0; input < columnCounts.Length; input++)
                {
                    messages.Add(new RoundMessage<StreamEventBatch>(input, Batch(new[] { Enumerable.Repeat(1L, columnCounts[input]).ToArray() }, new[] { 1 })));
                }
                // A task that ends canceled is a failure of the round too, its batch was not stored
                Exception injected = canceled ? new OperationCanceledException("Injected cancel") : new IOException("Injected failure");
                Action<int> hook = input =>
                {
                    if (input == 1)
                    {
                        throw injected;
                    }
                };
                if (inItsRead)
                {
                    MultiJoinOperator.RoundReadHookForTests = hook;
                }
                else
                {
                    MultiJoinOperator.RoundStoreHookForTests = hook;
                }

                try
                {
                    var thrown = await Assert.ThrowsAnyAsync<Exception>(() => Collect(op.OnRecieveRound(messages, 0), new Dictionary<string, int>()).WaitAsync(TimeSpan.FromSeconds(60)));

                    Assert.Contains("Injected", thrown.ToString());
                }
                finally
                {
                    foreach (var message in messages)
                    {
                        message.Message.Return();
                    }
                }
            }
            finally
            {
                MultiJoinOperator.RoundStoreHookForTests = hookBefore;
                MultiJoinOperator.RoundReadHookForTests = readHookBefore;
                await MultiJoinAllocationTests.DisposeAll(hosts);
            }
        }

        // The plans after the first two have joins that keep unmatched rows, their batches are put together and taken in turn
        [Theory]
        [InlineData("star")]
        [InlineData("chain")]
        [InlineData("left:star")]
        [InlineData("left:chain")]
        [InlineData("right:chain")]
        [InlineData("outer:chain")]
        [InlineData("markRoot")]
        [InlineData("checkUnderLeft")]
        public async Task SeveralBatchesOfOneInputInARoundAreTakenTogether(string planName)
        {
            var hosts = new List<Host>();
            try
            {
                var (oneByOne, firstHost) = await Create(planName);
                hosts.Add(firstHost);
                var (inRounds, secondHost) = await Create(planName);
                hosts.Add(secondHost);
                var columnCounts = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan(planName), 9)!.Inputs.Select(x => x.ColumnCount).ToArray();
                var random = new Random(11);
                var expected = new Dictionary<string, int>();
                var actual = new Dictionary<string, int>();
                for (int round = 1; round <= 6; round++)
                {
                    var messages = new List<RoundMessage<StreamEventBatch>>();
                    // Three batches per input, in turn
                    for (int m = 0; m < 3; m++)
                    {
                        for (int input = 0; input < columnCounts.Length; input++)
                        {
                            var rows = new long[4][];
                            var weights = new int[4];
                            for (int r = 0; r < 4; r++)
                            {
                                rows[r] = Enumerable.Range(0, columnCounts[input]).Select(_ => (long)random.Next(4)).ToArray();
                                weights[r] = 1;
                            }
                            messages.Add(new RoundMessage<StreamEventBatch>(input, Batch(rows, weights)));
                            var copy = Batch(rows, weights);
                            await Collect(oneByOne.OnRecieve(input, copy, 0), expected);
                            copy.Return();
                        }
                    }
                    await Collect(inRounds.OnRecieveRound(messages, 0), actual);
                    foreach (var message in messages)
                    {
                        message.Message.Return();
                    }
                    Assert.True(expected.Count == actual.Count && expected.All(x => actual.GetValueOrDefault(x.Key) == x.Value), $"Round {round} differs");
                }
                Assert.NotEmpty(expected);
            }
            finally
            {
                await MultiJoinAllocationTests.DisposeAll(hosts);
            }
        }
    }
}
