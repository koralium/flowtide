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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

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
        private EventBatchData? _actualData;
        int updateCounter = 0;
        int waitCounter = 0;
        FlowtideBuilder flowtideBuilder;
        private int _egressCrashOnCheckpointCount;
        private IPersistentStorage? _persistentStorage;
        private ConnectorManager? _connectorManager;
        private bool _dataUpdated;
        private NotificationReciever? _notificationReciever;
        private Watermark? _lastWatermark;

        public string TestName => testName;

        public IReadOnlyList<User> Users  => generator.Users;

        public IReadOnlyList<Order> Orders => generator.Orders;

        public IReadOnlyList<Company> Companies => generator.Companies;

        public IReadOnlyList<Project> Projects => generator.Projects;

        public IReadOnlyList<ProjectMember> ProjectMembers => generator.ProjectMembers;

        public IFunctionsRegister FunctionsRegister => flowtideBuilder.FunctionsRegister;

        public ISqlFunctionRegister SqlFunctionRegister => sqlPlanBuilder.FunctionRegister;

        public SqlPlanBuilder SqlPlanBuilder => sqlPlanBuilder;

        public int CachePageCount { get; set; } = 1000;

        public Watermark? LastWatermark => _lastWatermark;

        public StreamStateValue State => _stream!.State;

        public FlowtideTestStream(string testName)
        {
            var streamName = testName.Replace("/", "_");
            _db = new Internal.MockDatabase();
            generator = new DatasetGenerator(_db);
            sqlPlanBuilder = new SqlPlanBuilder();
            flowtideBuilder = new FlowtideBuilder(streamName)
                .WithLoggerFactory(new LoggerFactory(new List<ILoggerProvider>() { new DebugLoggerProvider() }));
            this.testName = testName;
        }

        public virtual void RegisterTableProviders(Action<SqlPlanBuilder> action)
        {
            action(sqlPlanBuilder);
        }

        public virtual void Generate(int count = 1000)
        {
            generator.Generate(count);
        }

        public void GenerateUsers(int count = 1000)
        {
            generator.GenerateUsers(count);
        }

        public void GenerateOrders(int count = 1000)
        {
            generator.GenerateOrders(count);
        }

        public void GenerateCompanies(int count = 1)
        {
            generator.GenerateCompanies(count);
        }

        public void GenerateProjects(int count = 1000)
        {
            generator.GenerateProjects(count);
        }

        public void GenerateProjectMembers(int count = 1000)
        {
            generator.GenerateProjectMembers(count);
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

        public void AddOrUpdateCompany(Company company)
        {
            generator.AddOrUpdateCompany(company);
        }

        public void AddOrUpdateOrder(Order order)
        {
            generator.AddOrUpdateOrder(order);
        }

        public void AddOrUpdateProject(Project project)
        {
            generator.AddOrUpdateProject(project);
        }

        public void AddOrUpdateProjectMember(ProjectMember projectMember)
        {
            generator.AddOrUpdateProjectMember(projectMember);
        }


        [MemberNotNull(nameof(_connectorManager))]
        public void SetupConnectorManager()
        {
            if (_connectorManager == null)
            {
                _connectorManager = new ConnectorManager();
                AddReadResolvers(_connectorManager);
                AddWriteResolvers(_connectorManager);
            }
        }

        public async Task StartStream(
            string sql, 
            int parallelism = 1, 
            StateSerializeOptions? stateSerializeOptions = default, 
            TimeSpan? timestampInterval = default,
            int pageSize = 1024,
            bool ignoreSameDataCheck = false)
        {
            if (stateSerializeOptions == null)
            {
                stateSerializeOptions = new StateSerializeOptions();
            }

            if (timestampInterval == null)
            {
                timestampInterval = TimeSpan.FromSeconds(1);
            }

            SetupConnectorManager();

            foreach (var tableProvider in _connectorManager.GetTableProviders())
            {
                sqlPlanBuilder.AddTableProvider(tableProvider);
            }
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));

            sqlPlanBuilder.Sql(sql);
            var plan = sqlPlanBuilder.GetPlan();

            


#if DEBUG_WRITE

            var loggerFactory = LoggerFactory.Create(b =>
            {
                var logger = new LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .WriteTo.File($"debugwrite/{testName.Replace("/", "_")}.log")
                    .CreateLogger();
                b.AddSerilog(logger);
                b.AddDebug();
            });
#endif

            _persistentStorage = CreatePersistentStorage(testName, ignoreSameDataCheck);
            _notificationReciever = new NotificationReciever(CheckpointComplete);

            plan = Core.Optimizer.PlanOptimizer.Optimize(plan);

            var emitValidationVisitor = new EmitLengthValidatorVisitor();
            foreach (var relation in plan.Relations)
            {
                emitValidationVisitor.Visit(relation, default!);
            }

            flowtideBuilder
                .AddPlan(plan, false)
                .SetParallelism(parallelism)
#if DEBUG_WRITE
                .WithLoggerFactory(loggerFactory)
#endif
                .AddConnectorManager(_connectorManager)
                .WithCheckpointListener(_notificationReciever)
                .WithStateChangeListener(_notificationReciever)
                .WithFailureListener(_notificationReciever)
                .SetGetTimestampUpdateInterval(timestampInterval.Value)
                .WithStateOptions(new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = CachePageCount,
                    SerializeOptions = stateSerializeOptions,
                    PersistentStorage = _persistentStorage,
                    DefaultBPlusTreePageSize = pageSize,
                    //DefaultBPlusTreePageSizeBytes = 1,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{testName}/tmp"
                    }
                });
            var stream = flowtideBuilder.Build();
            _stream = stream;
            await _stream.StartAsync();
        }

        protected virtual IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            return new TestStorage(new Storage.FileCacheOptions()
            {
                DirectoryPath = $"./data/tempFiles/{testName}/persist",
                SegmentSize = 1024L * 1024 * 1024 * 64
            }, ignoreSameDataCheck, true);
        }

        private void OnDataUpdate(EventBatchData actualData)
        {
            lock (_lock)
            {
                _actualData = actualData;
                _dataUpdated = true;
            }
        }

        private void CheckpointComplete()
        {
            lock (_lock)
            {
                if (_dataUpdated)
                {
                    updateCounter++;
                }
                _dataUpdated = false;
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
                CheckForErrors();
            }
        }

        public async Task HealthyFor(TimeSpan time)
        {
            Debug.Assert(_stream != null);
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
                CheckForErrors();
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
            CheckForErrors();
        }

        private void CheckForErrors()
        {
            if (_notificationReciever != null && _notificationReciever._error)
            {
                if (_notificationReciever._exception != null)
                {
                    throw _notificationReciever._exception;
                }
                else
                {
                    throw new Exception("Unknown error occured in stream without exception");
                }
            }

        }

        public virtual async Task WaitForUpdate()
        {
            Debug.Assert(_stream != null);
            int currentCounter = waitCounter;

            var scheduler = _stream.Scheduler as DefaultStreamScheduler;
            while (updateCounter == currentCounter)
            {
                await scheduler!.Tick();
                await Task.Delay(10);
                CheckForErrors();
            }
            waitCounter = updateCounter;
        }

        protected virtual void AddReadResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddSource(new MockSourceFactory("*", _db));
        }

        protected virtual void AddWriteResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddSink(new MockSinkFactory("*", OnDataUpdate, _egressCrashOnCheckpointCount, OnWatermark));
        }

        protected virtual void OnWatermark(Watermark watermark)
        {
            _lastWatermark = watermark;
        }

        public void AssertCurrentDataEqual<T>(IEnumerable<T> data)
        {
            var expectedBatch = BatchConverter.ConvertToBatchSorted(data, GlobalMemoryManager.Instance);
            EventBatchAssertion.Equal(expectedBatch, _actualData!);
        }

        public EventBatchData GetActualRowsAsVectors()
        {
            Assert.NotNull(_actualData);
            return _actualData;
        }

        public async ValueTask DisposeAsync()
        {
            if (_stream != null)
            {
                await _stream.DisposeAsync();
            }
            if (_persistentStorage != null)
            {
                _persistentStorage.Dispose();
            }
        }

        public async Task Trigger(string triggerName)
        {
            await _stream!.CallTrigger(triggerName, default);
        }

        public StreamGraph GetDiagnosticsGraph()
        {
            return _stream!.GetDiagnosticsGraph();
        }

        public Task StopStream()
        {
            return _stream!.StopAsync();
        }

        public Task StartStream()
        {
            return _stream!.StartAsync();
        }
    }
}
