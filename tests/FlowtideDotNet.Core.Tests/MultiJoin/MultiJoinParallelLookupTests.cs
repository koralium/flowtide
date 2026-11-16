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
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;
using Host = FlowtideDotNet.Core.Tests.MultiJoin.MultiJoinAllocationTests.Host;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinParallelLookupTests
    {
        private const int Inputs = 5;
        private const int KeyCount = 40_000;

        private sealed record Result(Dictionary<string, int> Rows, long LookupsBeside, long ReadsThatWaited, Exception? Failure, int ReadsWaitingAtFailure);

        private static int Columns(int input) => input == 0 || input == Inputs - 1 ? 2 : 3;

        // A star of five inputs is loaded and stored, then keys spread over the first input are updated
        private static async Task<Result> UpdateTheCentre(int cachePages, int readDelayMs, long failingRead = 0)
        {
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            var hosts = new List<Host>();
            var beside = new Dictionary<Instrument, long>();
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Name.EndsWith("parallel_lookups", StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
            {
                lock (beside)
                {
                    // An observable counter reports its total, a plain one what was added
                    beside[instrument] = instrument.IsObservable ? value : beside.GetValueOrDefault(instrument) + value;
                }
            });
            listener.Start();
            // Operators of earlier tests may still report, only what is added from here on counts
            listener.RecordObservableInstruments();
            Dictionary<Instrument, long> before;
            lock (beside)
            {
                before = new Dictionary<Instrument, long>(beside);
            }
            try
            {
                Host.ReadDelayMs = 0;
                Host.FailingRead = 0;
                Host.CachePages = cachePages;
                var output = new BufferBlock<IStreamEvent>();
                var targets = new ITargetBlock<IStreamEvent>[Inputs];
                Assert.Equal(1, await MultiJoinAllocationTests.BuildMulti(MultiJoinAllocationTests.ChainOf(Inputs, true)[Inputs - 2], output, 16, hosts, targets, options));
                for (int k = 0; k < KeyCount; k += 512)
                {
                    for (int input = 0; input < Inputs; input++)
                    {
                        await targets[input].SendAsync(MultiJoinAllocationTests.CreateMessage(input, k, Math.Min(512, KeyCount - k), 0, Columns(input)));
                    }
                }
                foreach (var target in targets)
                {
                    await target.SendAsync(new Checkpoint(1, 2, 1));
                }
                await MultiJoinAllocationTests.Drain(output);

                var random = new Random(7);
                var used = new HashSet<int>();
                var messages = new List<StreamMessage<StreamEventBatch>>();
                for (int m = 0; m < 3; m++)
                {
                    var keys = new int[16];
                    for (int k = 0; k < keys.Length; k++)
                    {
                        int key;
                        do
                        {
                            key = random.Next(KeyCount);
                        }
                        while (!used.Add(key));
                        keys[k] = key;
                    }
                    messages.Add(MultiJoinAllocationTests.CreateUpdate(keys, Columns(0)));
                }

                long readsBefore = Interlocked.Read(ref Host.ReadsThatWaited);
                if (failingRead > 0)
                {
                    Host.FailingRead = readsBefore + failingRead;
                }
                Host.ReadDelayMs = readDelayMs;
                foreach (var message in messages)
                {
                    await targets[0].SendAsync(message);
                }
                foreach (var target in targets)
                {
                    await target.SendAsync(new Checkpoint(2, 3, 1));
                }
                var rows = new Dictionary<string, int>();
                Exception? failure = null;
                int waitingAtFailure = 0;
                var completion = ((MultiJoinOperator)hosts[0].Operator!).Completion;
                while (true)
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));
                    var receive = output.ReceiveAsync(timeout.Token);
                    var first = await Task.WhenAny(receive, completion);
                    if (first == completion && completion.IsFaulted)
                    {
                        // The receive that lost is ended, with its timer
                        timeout.Cancel();
                        await receive.ContinueWith(static x => _ = x.Exception, TaskScheduler.Default);
                        waitingAtFailure = Volatile.Read(ref Host.ReadsWaiting);
                        failure = completion.Exception;
                        break;
                    }
                    var received = await receive;
                    if (received is ICheckpointEvent)
                    {
                        break;
                    }
                    if (received is StreamMessage<StreamEventBatch> message)
                    {
                        var data = message.Data.Data;
                        var columns = data.EventBatchData.Columns;
                        for (int r = 0; r < data.Weights.Count; r++)
                        {
                            var row = string.Join(",", columns.Select(c => c.GetValueAt(r, default).AsLong));
                            rows[row] = rows.GetValueOrDefault(row) + data.Weights[r];
                        }
                        data.Return();
                    }
                }
                Host.ReadDelayMs = 0;
                listener.RecordObservableInstruments();
                long lookupsBeside;
                lock (beside)
                {
                    lookupsBeside = beside.Sum(x => x.Value - before.GetValueOrDefault(x.Key));
                }
                return new Result(rows, lookupsBeside, Interlocked.Read(ref Host.ReadsThatWaited) - readsBefore, failure, waitingAtFailure);
            }
            finally
            {
                Host.ReadDelayMs = 0;
                Host.FailingRead = 0;
                Host.CachePages = null;
                await MultiJoinAllocationTests.DisposeAll(hosts);
            }
        }

        private static async Task<Result> WithLookupsInTurn(Func<Task<Result>> run)
        {
            bool before = MultiJoinOperator.ParallelLookups;
            MultiJoinOperator.ParallelLookups = false;
            try
            {
                return await run();
            }
            finally
            {
                MultiJoinOperator.ParallelLookups = before;
            }
        }

        [Fact]
        public async Task LookupsThatWaitForPagesRunBesideEachOther()
        {
            var inTurn = await WithLookupsInTurn(() => UpdateTheCentre(48, 1));
            var beside = await UpdateTheCentre(48, 1);

            Assert.True(inTurn.ReadsThatWaited > 0, "No page read waited, the cache holds the state");
            Assert.Equal(0, inTurn.LookupsBeside);
            Assert.True(beside.LookupsBeside > 0, "No lookup ran beside another");
            // Three messages of 16 keys, each gives the old row back and the new one
            Assert.Equal(96, inTurn.Rows.Count);
            Assert.Equal(inTurn.Rows, beside.Rows);
        }

        [Fact]
        public async Task LookupsThatDoNotWaitRunInTurn()
        {
            // The cache holds the whole state and the checkpoint is not still writing it, no lookup has a page to wait for
            Host.BackgroundCommit = false;
            Result result;
            try
            {
                result = await UpdateTheCentre(10000, 0);
            }
            finally
            {
                Host.BackgroundCommit = null;
            }

            Assert.Equal(0, result.LookupsBeside);
            Assert.Equal(96, result.Rows.Count);
        }

        [Fact]
        public async Task ALookupThatFindsMoreRowsThanAreKeptIsLookedUpInItsTurn()
        {
            var inTurn = await WithLookupsInTurn(() => UpdateTheCentre(48, 1));
            int maxBefore = MultiJoinOperator.ParallelLookupMaxRows;
            MultiJoinOperator.ParallelLookupMaxRows = 1;
            Result beside;
            try
            {
                beside = await UpdateTheCentre(48, 1);
            }
            finally
            {
                MultiJoinOperator.ParallelLookupMaxRows = maxBefore;
            }

            Assert.True(beside.LookupsBeside > 0, "No lookup ran beside another");
            Assert.Equal(inTurn.Rows, beside.Rows);
        }

        [Fact]
        public async Task AFailedPageReadFailsTheOperatorOnceNoLookupIsReading()
        {
            var result = await UpdateTheCentre(48, 1, failingRead: 3);

            Assert.NotNull(result.Failure);
            Assert.Contains("Injected page read failure", result.Failure!.ToString());
            Assert.Equal(0, result.ReadsWaitingAtFailure);
        }
    }
}
