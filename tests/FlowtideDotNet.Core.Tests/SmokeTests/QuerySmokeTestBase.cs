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

using FASTER.core;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Metrics.Gauge;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Tests.SmokeTests.Count;
using FlowtideDotNet.Core.Tests.SmokeTests.LineItemLeftJoinOrders;
using FlowtideDotNet.Core.Tests.SmokeTests.StringJoin;
using FlowtideDotNet.Storage.DeviceFactories;
using FlowtideDotNet.Substrait;
using Microsoft.Extensions.Logging;
using Serilog;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Tests.SmokeTests
{
    public abstract class QuerySmokeTestBase : IAsyncLifetime
    {
        FlowtideBuilder differentialComputeBuilder;
        SubstraitDeserializer deserializer;
        FlowtideDotNet.Base.Engine.DataflowStream? dataflowStream;
        private int changesCounter = 0;
        private DefaultStreamScheduler? _streamScheduler;
        //List<LineItem>? actualData;
        public QuerySmokeTestBase()
        {
            differentialComputeBuilder = new FlowtideBuilder("teststream");
            deserializer = new SubstraitDeserializer();
        }

        protected abstract void AddReadResolvers(IConnectorManager connectorManager);

        public async Task DisposeAsync()
        {
            if (dataflowStream != null)
            {
                await dataflowStream.DisposeAsync();
            }
            await ClearAllTables();
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        private async Task StartStream<TResult>(string testName, string planLocation, Action<List<TResult>> datachange, List<int> primaryKeysOutput, PlanOptimizerSettings? settings = null)
        {
            differentialComputeBuilder = new FlowtideBuilder(testName);
            var plantext = File.ReadAllText(planLocation);
            var plan = deserializer.Deserialize(plantext);
            PlanModifier planModifier = new PlanModifier();
            planModifier.AddRootPlan(plan);
            // Supress since write to table is required for this test
#pragma warning disable CS0618 // Type or member is obsolete
            planModifier.WriteToTable("testoutput");
#pragma warning restore CS0618 // Type or member is obsolete
            var modifiedPlan = planModifier.Modify();

            modifiedPlan = PlanOptimizer.Optimize(modifiedPlan, settings);

            ConnectorManager connectorManager = new ConnectorManager();
            AddReadResolvers(connectorManager);

            _streamScheduler = new DefaultStreamScheduler();
            connectorManager.AddSink(new TestWriteOperatorFactory<TResult>("*", primaryKeysOutput, (rows) =>
            {
                changesCounter++;
                datachange(rows);
                return Task.CompletedTask;
            }));

            var loggerFactory = LoggerFactory.Create(b =>
            {
                var logger = new LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .WriteTo.File($"debugwrite/{testName.Replace("/", "_")}.log")
                    .CreateLogger();
                b.AddSerilog(logger);
                b.AddDebug();
            });

            var checkpointManager = new DeviceLogCommitCheckpointManager(
                new InMemoryDeviceFactory(),
                new DefaultCheckpointNamingScheme($"checkpoints/"));
            var logDevice = new ManagedLocalStorageDevice("logdevice", deleteOnClose: true);
            dataflowStream = differentialComputeBuilder
                .AddPlan(modifiedPlan, false)
                .AddConnectorManager(connectorManager)
                .WithScheduler(_streamScheduler)
                .WithLoggerFactory(loggerFactory)
                .WithStateOptions(new FlowtideDotNet.Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100000
                })
                .Build();

            await dataflowStream.StartAsync();
        }

        [Fact]
        public async Task SelectLineItems()
        {
            // add all line items
            await AddLineItems(TpchData.GetLineItems());
            List<LineItem>? actualData = default;
            await StartStream<LineItem>("SelectLineItems", "./SmokeTests/SelectLineItems/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 });

            while (changesCounter == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var lineItemsExpected = TpchData.GetLineItems().OrderBy(x => x.Orderkey).ThenBy(x => x.Linenumber).ToList();
            Assert.Equal(lineItemsExpected.Count, actualData!.Count);
            for (int i = 0; i < lineItemsExpected.Count; i++)
            {
                Assert.Equivalent(lineItemsExpected[i], actualData[i]);
            }
        }

        [Fact]
        public async Task CountLineItems()
        {
            // add all line items
            var lineItems = TpchData.GetLineItems();
            await AddLineItems(lineItems.Take(1000));
            List<CountResult>? actualData = default;
            await StartStream<CountResult>("CountLineItems", "./SmokeTests/Count/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0 });
            Debug.Assert(_streamScheduler != null);

            while (changesCounter == 0)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
            await AddLineItems(lineItems.Skip(1000).Take(1000));
            while (changesCounter == 1)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var lineItemsExpected = new List<CountResult>() { new CountResult() { Count = 2000 } };
            Assert.Equal(lineItemsExpected.Count, actualData!.Count);
            for (int i = 0; i < lineItemsExpected.Count; i++)
            {
                Assert.Equivalent(lineItemsExpected[i], actualData[i]);
            }
        }

        [Fact]
        public async Task FilterLineItemsOnShipmode()
        {
            await AddLineItems(TpchData.GetLineItems());
            List<LineItem>? actualData = default;
            await StartStream<LineItem>("FilterLineItemsOnShipmode", "./SmokeTests/FilterLineItemsOnShipmode/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 });

            while (changesCounter == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var lineItemsExpected = TpchData.GetLineItems()
                .OrderBy(x => x.Orderkey)
                .ThenBy(x => x.Linenumber)
                .Where(x => x.Shipmode!.Equals("truck", StringComparison.OrdinalIgnoreCase))
                .ToList();
            Assert.Equal(lineItemsExpected.Count, actualData!.Count);
            for (int i = 0; i < lineItemsExpected.Count; i++)
            {
                Assert.Equivalent(lineItemsExpected[i], actualData[i]);
            }
        }

        [Fact]
        public async Task LineItemLeftJoinOrders()
        {
            await AddLineItems(TpchData.GetLineItems());
            await AddOrders(TpchData.GetOrders());
            List<LineItemJoinOrderResult>? actualData = default;
            await StartStream<LineItemJoinOrderResult>("LineItemLeftJoinOrders", "./SmokeTests/LineItemLeftJoinOrders/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 });

            while (changesCounter == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = TpchData.GetLineItems()
                .Join(TpchData.GetOrders(), x => x.Orderkey, x => x.Orderkey, (l, r) =>
                {
                    return new LineItemJoinOrderResult()
                    {
                        Orderkey = l.Orderkey,
                        Linenumber = l.Linenumber,
                        Quantity = l.Quantity,
                        Custkey = r.Custkey,
                        Orderstatus = r.Orderstatus!,
                    };
                })
                .OrderBy(x => x.Orderkey)
                .ThenBy(x => x.Linenumber)
                .ToList();

            actualData = actualData!.OrderBy(x => x.Orderkey).ThenBy(x => x.Linenumber).ToList();

            Assert.Equal(expectedData.Count, actualData!.Count);
            for (int i = 0; i < expectedData.Count; i++)
            {
                Assert.Equivalent(expectedData[i], actualData[i]);
            }
        }

        //[Fact(Skip = "Takes a long time to run, can be run manually if required."), Trait("Category", "NLJ")]
        [Fact]
        public async Task LineItemLeftJoinOrdersNLJ()
        {
            await AddLineItems(TpchData.GetLineItems());
            await AddOrders(TpchData.GetOrders());
            List<LineItemJoinOrderResult>? actualData = default;
            await StartStream<LineItemJoinOrderResult>("LineItemLeftJoinOrdersNLJ", "./SmokeTests/LineItemLeftJoinOrders/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 }, new PlanOptimizerSettings() { NoMergeJoin = true });
            Debug.Assert(_streamScheduler != null);

            while (changesCounter == 0)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = TpchData.GetLineItems()
                .Join(TpchData.GetOrders(), x => x.Orderkey, x => x.Orderkey, (l, r) =>
                {
                    return new LineItemJoinOrderResult()
                    {
                        Orderkey = l.Orderkey,
                        Linenumber = l.Linenumber,
                        Quantity = l.Quantity,
                        Custkey = r.Custkey,
                        Orderstatus = r.Orderstatus!,
                    };
                })
                .OrderBy(x => x.Orderkey)
                .ThenBy(x => x.Linenumber)
                .ToList();

            actualData = actualData!.OrderBy(x => x.Orderkey).ThenBy(x => x.Linenumber).ToList();

            Assert.Equal(expectedData.Count, actualData!.Count);
            for (int i = 0; i < expectedData.Count; i++)
            {
                Assert.Equivalent(expectedData[i], actualData[i]);
            }
        }

        protected abstract Task ClearAllTables();

        protected abstract Task AddLineItems(IEnumerable<LineItem> lineItems);

        protected abstract Task AddOrders(IEnumerable<Order> orders);

        protected abstract Task AddShipmodes(IEnumerable<Shipmode> shipmodes);

        protected abstract Task UpdateShipmodes(IEnumerable<Shipmode> shipmode);

        protected abstract Task Crash();

        protected abstract Task Restart();

        [Fact]
        public async Task LineItemInnerJoinOrders()
        {
            await AddLineItems(TpchData.GetLineItems());
            await AddOrders(TpchData.GetOrders());
            List<LineItemJoinOrderResult>? actualData = default;
            await StartStream<LineItemJoinOrderResult>("LineItemInnerJoinOrders", "./SmokeTests/LineItemInnerJoinOrders/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 });

            while (changesCounter == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = TpchData.GetLineItems()
                .Join(TpchData.GetOrders(), x => x.Orderkey, x => x.Orderkey, (l, r) =>
                {
                    return new LineItemJoinOrderResult()
                    {
                        Orderkey = l.Orderkey,
                        Linenumber = l.Linenumber,
                        Quantity = l.Quantity,
                        Custkey = r.Custkey,
                        Orderstatus = r.Orderstatus!,
                    };
                })
                .OrderBy(x => x.Orderkey)
                .ThenBy(x => x.Linenumber)
                .ToList();

            actualData = actualData!.OrderBy(x => x.Orderkey).ThenBy(x => x.Linenumber).ToList();

            Assert.Equal(expectedData.Count, actualData!.Count);
            for (int i = 0; i < expectedData.Count; i++)
            {
                Assert.Equivalent(expectedData[i], actualData[i]);
            }
        }

        [Fact(Skip = "Takes a long time to run, can be run manually if required."), Trait("Category", "NLJ")]
        public async Task LineItemInnerJoinOrdersNLJ()
        {
            await AddLineItems(TpchData.GetLineItems());
            await AddOrders(TpchData.GetOrders());
            List<LineItemJoinOrderResult>? actualData = default;
            await StartStream<LineItemJoinOrderResult>("LineItemInnerJoinOrdersNLJ", "./SmokeTests/LineItemInnerJoinOrders/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 }, new PlanOptimizerSettings() { NoMergeJoin = true });

            while (changesCounter == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = TpchData.GetLineItems()
                .Join(TpchData.GetOrders(), x => x.Orderkey, x => x.Orderkey, (l, r) =>
                {
                    return new LineItemJoinOrderResult()
                    {
                        Orderkey = l.Orderkey,
                        Linenumber = l.Linenumber,
                        Quantity = l.Quantity,
                        Custkey = r.Custkey,
                        Orderstatus = r.Orderstatus!,
                    };
                })
                .OrderBy(x => x.Orderkey)
                .ThenBy(x => x.Linenumber)
                .ToList();

            actualData = actualData!.OrderBy(x => x.Orderkey).ThenBy(x => x.Linenumber).ToList();

            Assert.Equal(expectedData.Count, actualData!.Count);
            for (int i = 0; i < expectedData.Count; i++)
            {
                Assert.Equivalent(expectedData[i], actualData[i]);
            }
        }

        [Fact]
        public async Task StringJoin()
        {
            await AddLineItems(TpchData.GetLineItems());
            await AddShipmodes(TpchData.GetShipmodes());
            List<StringJoinResult>? actualData = default;
            await StartStream<StringJoinResult>("StringJoin", "./SmokeTests/StringJoin/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 });

            while (changesCounter == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = TpchData.GetLineItems()
                .Join(TpchData.GetShipmodes(), x => x.Shipmode, x => x.Mode, (l, r) =>
                {
                    return new StringJoinResult()
                    {
                        Orderkey = l.Orderkey,
                        Linenumber = l.Linenumber,
                        Cost = r.Cost
                    };
                })
                .OrderBy(x => x.Orderkey)
                .ThenBy(x => x.Linenumber)
                .ToList();

            actualData = actualData!.OrderBy(x => x.Orderkey).ThenBy(x => x.Linenumber).ToList();

            Assert.Equal(expectedData.Count, actualData!.Count);
            for (int i = 0; i < expectedData.Count; i++)
            {
                Assert.Equivalent(expectedData[i], actualData[i]);
            }
        }

        [Fact]
        public async Task LeftJoinUpdateLeftValues()
        {
            var truck = TpchData.GetShipmodes().First(x => x.Mode == "TRUCK");
            await AddLineItems(new List<LineItem>() { TpchData.GetLineItems().First(x => x.Shipmode == "TRUCK") });

            List<StringJoinResult>? actualData = default;
            await StartStream<StringJoinResult>("LeftJoinUpdateLeftValues", "./SmokeTests/LeftJoinUpdateLeftValues/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 });
            Debug.Assert(_streamScheduler != null);

            while (changesCounter < 1)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = new StringJoinResult()
            {
                Cost = null,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.NotNull(actualData);
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);

            await AddShipmodes(new List<Shipmode>() { truck });

            while (changesCounter < 2)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            expectedData = new StringJoinResult()
            {
                Cost = 10,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);

            await UpdateShipmodes(new List<Shipmode>(){ new Shipmode()
            {
                ShipmodeKey = truck.ShipmodeKey,
                Cost = truck.Cost,
                Mode = "truck2"
            } });

            while (changesCounter < 3)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            expectedData = new StringJoinResult()
            {
                Cost = null,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);

            await UpdateShipmodes(new List<Shipmode>() { truck });

            while (changesCounter < 4)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            expectedData = new StringJoinResult()
            {
                Cost = 10,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);
        }

        [Fact]
        public async Task LeftJoinUpdateLeftValuesNLJ()
        {
            var truck = TpchData.GetShipmodes().First(x => x.Mode == "TRUCK");
            await AddLineItems(new List<LineItem>() { TpchData.GetLineItems().First(x => x.Shipmode == "TRUCK") });

            List<StringJoinResult>? actualData = default;
            await StartStream<StringJoinResult>("LeftJoinUpdateLeftValuesNLJ", "./SmokeTests/LeftJoinUpdateLeftValues/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 }, new PlanOptimizerSettings() { NoMergeJoin = true });
            Debug.Assert(_streamScheduler != null);

            while (changesCounter < 1)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = new StringJoinResult()
            {
                Cost = null,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.NotNull(actualData);
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);

            await AddShipmodes(new List<Shipmode>() { truck });

            while (changesCounter < 2)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            expectedData = new StringJoinResult()
            {
                Cost = 10,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);

            await UpdateShipmodes(new List<Shipmode>(){ new Shipmode()
            {
                ShipmodeKey = truck.ShipmodeKey,
                Cost = truck.Cost,
                Mode = "truck2"
            } });

            while (changesCounter < 3)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            expectedData = new StringJoinResult()
            {
                Cost = null,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);

            await UpdateShipmodes(new List<Shipmode>() { truck });

            while (changesCounter < 4)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            expectedData = new StringJoinResult()
            {
                Cost = 10,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);
        }

        [Fact]
        public async Task InnerJoinUpdateLeftValuesNLJ()
        {
            var truck = TpchData.GetShipmodes().First(x => x.Mode == "TRUCK");
            await AddLineItems(new List<LineItem>() { TpchData.GetLineItems().First(x => x.Shipmode == "TRUCK") });

            List<StringJoinResult>? actualData = default;
            await StartStream<StringJoinResult>("InnerJoinUpdateLeftValuesNLJ", "./SmokeTests/StringJoin/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 }, new PlanOptimizerSettings() { NoMergeJoin = true });
            Debug.Assert(_streamScheduler != null);

            while (changesCounter < 1)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            Assert.Empty(actualData!);

            await AddShipmodes(new List<Shipmode>() { truck });

            while (changesCounter < 2)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            var expectedData = new StringJoinResult()
            {
                Cost = 10,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.NotNull(actualData);
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);

            await UpdateShipmodes(new List<Shipmode>(){ new Shipmode()
            {
                ShipmodeKey = truck.ShipmodeKey,
                Cost = truck.Cost,
                Mode = "truck2"
            } });

            while (changesCounter < 3)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            Assert.Empty(actualData);

            await UpdateShipmodes(new List<Shipmode>() { truck });

            while (changesCounter < 4)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            expectedData = new StringJoinResult()
            {
                Cost = 10,
                Linenumber = 1,
                Orderkey = 14977
            };
            Assert.Single(actualData);
            Assert.Equivalent(expectedData, actualData[0]);
        }

        [Fact]
        public async Task InnerJoinWithCrash()
        {
            var truck = TpchData.GetShipmodes().First(x => x.Mode == "TRUCK");
            await AddLineItems(new List<LineItem>() { TpchData.GetLineItems().First(x => x.Shipmode == "TRUCK") });

            List<StringJoinResult>? actualData = default;
            await StartStream<StringJoinResult>("InnerJoinWithCrash", "./SmokeTests/StringJoin/queryplan.json", rows =>
            {
                actualData = rows;
            }, new List<int>() { 0, 1 });
            Debug.Assert(_streamScheduler != null);
            Debug.Assert(dataflowStream != null);

            while (changesCounter < 1)
            {
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            await Crash();

            var graph = dataflowStream.GetDiagnosticsGraph();
            while (dataflowStream.State == Base.Engine.Internal.StateMachine.StreamStateValue.Running && graph.State != Base.Engine.Internal.StateMachine.StreamStateValue.Failure)
            {
                graph = dataflowStream.GetDiagnosticsGraph();
                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }

            await Restart();

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            while (true)
            {
                if (dataflowStream.State == Base.Engine.Internal.StateMachine.StreamStateValue.Running)
                {
                    graph = dataflowStream.GetDiagnosticsGraph();

                    var healthValues = new List<decimal>();
                    foreach (var gauge in graph.Nodes.Select(x => x.Value.Gauges.First(x => x.Name == "flowtide_health")))
                    {
                        healthValues.Add(gauge.Dimensions[""].Value);
                    }

                    if (healthValues.All(x => x == 1))
                    {
                        break;
                    }
                }

                if (stopwatch.Elapsed > TimeSpan.FromSeconds(60))
                {
                    Assert.Fail("Timed out waiting for stream to become healthy");
                }

                await _streamScheduler.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }
    }
}
