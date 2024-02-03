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

using FastMember;
using FlexBuffers;
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
using System.Buffers;
using System.Diagnostics;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public class FlowtideTestStream : IAsyncDisposable
    {
        private MockDatabase _db;
        private DatasetGenerator generator;
        private SqlPlanBuilder sqlPlanBuilder;
        private Base.Engine.DataflowStream? _stream;
        private readonly object _lock = new object();
        private readonly string testName;
        private List<byte[]>? _actualData;
        int updateCounter = 0;
        FlowtideBuilder flowtideBuilder;
        private int _egressCrashOnCheckpointCount;
        private FileCachePersistentStorage? _fileCachePersistence;

        public IReadOnlyList<User> Users  => generator.Users;

        public IReadOnlyList<Order> Orders => generator.Orders;

        public IReadOnlyList<Company> Companies => generator.Companies;

        public IFunctionsRegister FunctionsRegister => flowtideBuilder.FunctionsRegister;

        public ISqlFunctionRegister SqlFunctionRegister => sqlPlanBuilder.FunctionRegister;

        public SqlPlanBuilder SqlPlanBuilder => sqlPlanBuilder;

        public FlowtideTestStream(string testName)
        {
            var streamName = testName.Replace("/", "_");
            _db = new Internal.MockDatabase();
            generator = new DatasetGenerator(_db);
            sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
            flowtideBuilder = new FlowtideBuilder(streamName)
                .WithLoggerFactory(new LoggerFactory(new List<ILoggerProvider>() { new DebugLoggerProvider() }));
            this.testName = testName;
        }

        public virtual void RegisterTableProviders(Action<SqlPlanBuilder> action)
        {
            action(sqlPlanBuilder);
        }

        public void Generate(int count = 1000)
        {
            generator.Generate(count);
        }

        public void AddOrUpdateUser(User user)
        {
            generator.AddOrUpdateUser(user);
        }

        public void DeleteUser(User user)
        {
            generator.DeleteUser(user);
        }

        public void DeleteOrder(Order order)
        {
            generator.DeleteOrder(order);
        }

        public async Task StartStream(string sql, int parallelism = 1, StateSerializeOptions? stateSerializeOptions = default, TimeSpan? timestampInterval = default)
        {
            if (stateSerializeOptions == null)
            {
                stateSerializeOptions = new StateSerializeOptions();
            }

            if (timestampInterval == null)
            {
                timestampInterval = TimeSpan.FromSeconds(1);
            }
            sqlPlanBuilder.Sql(sql);
            var plan = sqlPlanBuilder.GetPlan();

            var factory = new ReadWriteFactory();
            AddReadResolvers(factory);
            AddWriteResolvers(factory);

            _fileCachePersistence = new FileCachePersistentStorage(new Storage.FileCacheOptions()
            {
                DirectoryPath = $"./data/tempFiles/{testName}/persist",
                SegmentSize = 1024L * 1024 * 1024 * 64
            }, true);

            flowtideBuilder
                .AddPlan(plan)
                .SetParallelism(parallelism)
                .AddReadWriteFactory(factory)
                .SetGetTimestampUpdateInterval(timestampInterval.Value)
                .WithStateOptions(new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 1000,
                    SerializeOptions = stateSerializeOptions,
                    PersistentStorage = _fileCachePersistence,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{testName}/tmp"
                    }
                });
            var stream = flowtideBuilder.Build();
            _stream = stream;
            await _stream.StartAsync();
        }

        private void OnDataUpdate(List<byte[]> actualData)
        {
            lock (_lock)
            {
                _actualData = actualData;
                updateCounter++;
            }
        }

        /// <summary>
        /// Simulate a crash on the stream, waits until the stream has failed.
        /// </summary>
        /// <returns></returns>
        public async Task Crash()
        {
            await _stream!.CallTrigger("crash", default);

            var graph = _stream.GetDiagnosticsGraph();
            var scheduler = _stream.Scheduler as DefaultStreamScheduler;
            while (_stream.State == Base.Engine.Internal.StateMachine.StreamStateValue.Running && graph.State != Base.Engine.Internal.StateMachine.StreamStateValue.Failure)
            {
                graph = _stream.GetDiagnosticsGraph();
                await scheduler!.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }

        public async Task HealthyFor(TimeSpan time)
        {
            var graph = _stream.GetDiagnosticsGraph();
            var scheduler = _stream.Scheduler as DefaultStreamScheduler;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            while (_stream.State == Base.Engine.Internal.StateMachine.StreamStateValue.Running && graph.State != Base.Engine.Internal.StateMachine.StreamStateValue.Failure)
            {
                if (stopwatch.Elapsed.CompareTo(time) > 0)
                {
                    break;
                }
                graph = _stream.GetDiagnosticsGraph();
                await scheduler!.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
            if (graph.State != Base.Engine.Internal.StateMachine.StreamStateValue.Running || graph.State != Base.Engine.Internal.StateMachine.StreamStateValue.Running)
            {
                Assert.Fail("Stream failed");
            }
        }

        public void EgressCrashOnCheckpoint(int times)
        {
            _egressCrashOnCheckpointCount = times;
        }

        public async Task SchedulerTick()
        {
            if (_stream == null)
            {
                throw new InvalidOperationException("Stream must be started first.");
            }
            var scheduler = _stream.Scheduler as DefaultStreamScheduler;
            await scheduler!.Tick();
        }

        public async Task WaitForUpdate()
        {
            Debug.Assert(_stream != null);
            int currentCounter = 0;
            lock (_lock)
            {
                currentCounter = updateCounter;
            }
            var scheduler = _stream.Scheduler as DefaultStreamScheduler;
            while (updateCounter == currentCounter)
            {
                await scheduler!.Tick();
                await Task.Delay(10);
            }
        }

        protected virtual void AddReadResolvers(ReadWriteFactory factory)
        {
            factory.AddMockSource("*", _db);
        }

        protected virtual void AddWriteResolvers(ReadWriteFactory factory)
        {
            factory.AddWriteResolver((rel, opt) =>
            {
                return new MockDataSink(opt, OnDataUpdate, _egressCrashOnCheckpointCount);
            });
        }

        public void AssertCurrentDataEqual<T>(IEnumerable<T> data)
        {
            var membersInOrder = typeof(T).GetProperties().Select(x => x.Name).ToList();
            var accessor = TypeAccessor.Create(typeof(T));

            SortedDictionary<RowEvent, int> dict = new SortedDictionary<RowEvent, int>(new BPlusTreeStreamEventComparer());

            foreach (var row in data)
            {
                Assert.NotNull(row);
                var e = MockTable.ToStreamEvent(new RowOperation(row, false), membersInOrder);
                if (dict.TryGetValue(e, out var weight))
                {
                    dict[e] = e.Weight + weight;
                }
                else
                {
                    dict.Add(e, 1);
                }
            }
            var expectedData = dict.SelectMany(x =>
            {
                List<byte[]> output = new List<byte[]>();
                for (int i = 0; i < x.Value; i++)
                {
                    var compactData = (CompactRowData)x.Key.Compact(new FlexBuffer(ArrayPool<byte>.Shared)).RowData;
                    output.Add(compactData.Span.ToArray());
                }
                return output;
            }).ToList();

            Assert.Equal(expectedData.Count, _actualData!.Count);

            bool fail = false;
            for (int i = 0; i < expectedData.Count; i++)
            {
                var expectedRow = expectedData[i];
                var actualRow = _actualData[i];

                if (!expectedRow.SequenceEqual(actualRow))
                {
                    var expectedRowJson = FlxValue.FromMemory(expectedRow).ToJson;
                    var actualRowJson = FlxValue.FromMemory(actualRow).ToJson;
                    if (!expectedRowJson.Equals(actualRowJson))
                    {
                        fail = true;
                    }
                }
            }

            if (fail)
            {
                List<string> expected = new List<string>();
                List<string> actual = new List<string>();

                for (int i = 0; i < expectedData.Count; i++)
                {
                    var expectedRow = expectedData[i];
                    var actualRow = _actualData[i];
                    expected.Add(FlxValue.FromMemory(expectedRow).ToJson);
                    actual.Add(FlxValue.FromMemory(actualRow).ToJson);
                }
                expected.Sort();
                actual.Sort();
                for (int i = 0; i < expected.Count; i++)
                {
                    Assert.Equal(expected[i], actual[i]);
                }
            }
        }

        public List<FlxVector> GetActualRowsAsVectors()
        {
            Assert.NotNull(_actualData);
            List<FlxVector> output = new List<FlxVector>();
            for(int i = 0; i < _actualData.Count; i++)
            {
                output.Add(FlxValue.FromMemory(_actualData[i]).AsVector);
            }
            return output;
        }

        public async ValueTask DisposeAsync()
        {
            if (_stream != null)
            {
                await _stream.DisposeAsync();
            }
            if (_fileCachePersistence != null)
            {
                _fileCachePersistence.ForceDispose();
            }
        }

        public async Task Trigger(string triggerName)
        {
            await _stream!.CallTrigger(triggerName, default);
        }
    }
}
