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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Substrait.Sql;
using System.Collections.Concurrent;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    // Tests that change a process-wide engine static run alone, a concurrent stream would run under the changed value.
    [CollectionDefinition(Name, DisableParallelization = true)]
    public class ProcessWideEngineStaticsCollection
    {
        public const string Name = "Process-wide engine statics";
    }

    [Collection(ProcessWideEngineStaticsCollection.Name)]
    public class UnpairableBarrierTeardownTests : IAsyncLifetime
    {
        private readonly MockDatabase _db;
        private readonly DatasetGenerator _generator;
        private DistributedFlowtideStream? _stream;

        public UnpairableBarrierTeardownTests()
        {
            FastEngineTimings.Apply();
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            if (_stream != null)
            {
                await _stream.DisposeAsync();
            }
        }

        /// <summary>
        /// A barrier from the other substream that exhausts the pairing budget fails and
        /// recovers the stream, and the recovery must not deadlock the failure teardown:
        /// the rollback is initiated from inside the read operators own fetch task, which
        /// the teardown waits on. A stop issued after the recovery started must complete.
        /// </summary>
        [Fact]
        public async Task UnpairableBarrierRecoveryDoesNotDeadlockTeardown()
        {
            var originalDelay = Core.Operators.Exchange.SubstreamReadOperator.PairingAttemptDelay;
            Core.Operators.Exchange.SubstreamReadOperator.PairingAttemptDelay = TimeSpan.FromMilliseconds(100);
            try
            {
                _generator.Generate(100);

                var latestData = new ConcurrentDictionary<string, EventBatchData>();
                var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

                // substream_0's sources hold initial data far longer than the shortened
                // pairing budget (24 x 100ms), so substream_1's barrier cannot pair and the
                // read operator fails and recovers from inside its own fetch task.
                _stream = new DistributedStreamBuilder("e2e_unpairable_teardown")
                    .AddPlan(() =>
                    {
                        var sqlPlanBuilder = new SqlPlanBuilder();
                        sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                        sqlPlanBuilder.Sql(DistributedStreamE2ETests.NormalJoinSql);
                        return sqlPlanBuilder.GetPlan();
                    })
                    .WithStateOptionsFactory((streamName, substreamName) => DistributedStreamE2ETests.CreateStateOptions("e2e_unpairable_teardown", substreamName))
                    .ConfigureSubstream((substreamName, substreamBuilder) =>
                    {
                        var connectorManager = new ConnectorManager();
                        var delay = substreamName == "substream_0" ? TimeSpan.FromSeconds(30) : TimeSpan.Zero;
                        connectorManager.AddSource(new MockSourceFactory("*", _db, false, initialDataDelay: delay));
                        connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                        substreamBuilder.AddConnectorManager(connectorManager);
                        substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                    })
                    .DistributeAutomatically(2)
                    .Build();

                await _stream.StartAsync();

                // Wait until the pairing budget expiry has failed substream_0.
                var deadline = DateTime.UtcNow.AddSeconds(20);
                while (!failures.Any(f => f.Substream == "substream_0"))
                {
                    Assert.True(DateTime.UtcNow < deadline, "The pairing budget expiry never failed substream_0");
                    await Task.Delay(100);
                }

                // The stop must complete even though the recovery was initiated from inside
                // the fetch task the teardown waits on.
                var stopTask = _stream.StopAsync();
                var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(45)));
                Assert.True(finished == stopTask, "Stop hung after an unpairable barrier recovery, the failure teardown deadlocked");
                await stopTask;
            }
            finally
            {
                Core.Operators.Exchange.SubstreamReadOperator.PairingAttemptDelay = originalDelay;
            }
        }
    }
}
