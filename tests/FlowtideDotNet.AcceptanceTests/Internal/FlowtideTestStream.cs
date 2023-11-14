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
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
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

        public IReadOnlyList<User> Users  => generator.Users;

        public IReadOnlyList<Order> Orders => generator.Orders;

        public IReadOnlyList<Company> Companies => generator.Companies;

        public IFunctionsRegister FunctionsRegister => flowtideBuilder.FunctionsRegister;

        public ISqlFunctionRegister SqlFunctionRegister => sqlPlanBuilder.FunctionRegister;

        public FlowtideTestStream(string testName)
        {
            _db = new Internal.MockDatabase();
            generator = new DatasetGenerator(_db);
            sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
            flowtideBuilder = new FlowtideBuilder("stream")
                .WithLoggerFactory(new LoggerFactory(new List<ILoggerProvider>() { new DebugLoggerProvider() }));
            this.testName = testName;
        }

        public void Generate(int count = 1000)
        {
            generator.Generate(count);
        }

        public void AddOrUpdateUser(User user)
        {
            generator.AddOrUpdateUser(user);
        }

        public async Task StartStream(string sql, int parallelism = 1)
        {
            sqlPlanBuilder.Sql(sql);
            var plan = sqlPlanBuilder.GetPlan();

            var factory = new ReadWriteFactory();
            AddReadResolvers(factory);
            AddWriteResolvers(factory);

            flowtideBuilder
                .AddPlan(plan)
                .SetParallelism(parallelism)
                .AddReadWriteFactory(factory)
                .WithStateOptions(new Storage.StateManager.StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{testName}/persist",
                        SegmentSize = 1024L * 1024 * 1024 * 64
                    }),
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
                //if (_stream.Status == StreamStatus.Failing)
                //{
                //    throw new InvalidOperationException("Stream failed");
                //}
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
                return new MockDataSink(opt, OnDataUpdate);
            });
        }

        public void AssertCurrentDataEqual<T>(IEnumerable<T> data)
        {
            var membersInOrder = typeof(T).GetProperties().Select(x => x.Name).ToList();
            var accessor = TypeAccessor.Create(typeof(T));

            SortedDictionary<StreamEvent, int> dict = new SortedDictionary<StreamEvent, int>(new BPlusTreeStreamEventComparer());

            foreach (var row in data)
            {
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
                    output.Add(x.Key.Memory.ToArray());
                }
                return output;
            }).ToList();

            Assert.Equal(expectedData.Count, _actualData!.Count);

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
                        Assert.Fail($"Expected:{Environment.NewLine}{expectedRowJson}{Environment.NewLine}but got:{Environment.NewLine}{actualRowJson}");
                    }
                }
            }
        }

        public List<FlxVector> GetActualRowsAsVectors()
        {
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
        }
    }
}
