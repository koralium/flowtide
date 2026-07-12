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
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Serilog;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using System.Buffers;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.AcceptanceTests.Entities.tpcdi;

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

        public IReadOnlyList<User> Users => generator.Users;

        public IReadOnlyList<Order> Orders => generator.Orders;

        public IReadOnlyList<Company> Companies => generator.Companies;

        public IReadOnlyList<Project> Projects => generator.Projects;

        public IReadOnlyList<ProjectMember> ProjectMembers => generator.ProjectMembers;

        public IReadOnlyList<Entities.GraphNode> GraphNodes => generator.GraphNodes;

        public IReadOnlyList<Security> Securities => generator.Securities;

        public IReadOnlyList<DailyMarket> DailyMarkets => generator.DailyMarkets;

        public IFunctionsRegister FunctionsRegister => flowtideBuilder.FunctionsRegister;

        public ISqlFunctionRegister SqlFunctionRegister => sqlPlanBuilder.FunctionRegister;

        public SqlPlanBuilder SqlPlanBuilder => sqlPlanBuilder;

        public int CachePageCount { get; set; } = 0;

        /// <summary>
        /// Enables the stream option that takes a checkpoint right after initial data, which
        /// installs a checkpoint placeholder during startup. Set before starting the stream.
        /// </summary>
        public bool WaitForCheckpointAfterInitialData { get; set; }

        /// <summary>
        /// Delays every source's initial data send by this amount, keeping the stream in its
        /// starting phase so a test can act while startup is still in progress. Set before
        /// starting the stream.
        /// </summary>
        public TimeSpan? InitialDataDelay { get; set; }

        /// <summary>
        /// Sets the minimum time between checkpoint triggers. Set before starting the stream.
        /// </summary>
        public TimeSpan? MinimumTimeBetweenCheckpoints { get; set; }

        /// <summary>
        /// Sets the stop drain timeout, which also bounds a stop deferred behind an
        /// in-progress checkpoint. Set before starting the stream.
        /// </summary>
        public TimeSpan? StopDrainTimeout { get; set; }

        public int BPlusTreePageSizeBytes { get; set; } = 32 * 1024;

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

        public void EnterDataWriteLock()
        {
            for (int i = 0; i < 10; i++)
            {
                _db.RwLock.Wait();
            }
        }

        public void ExitDataWriteLock()
        {
            for (int i = 0; i < 10; i++)
            {
                _db.RwLock.Release();
            }
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

        public void GenerateGraphNodes(int count = 1000)
        {
            generator.GenerateGraphNodes(count);
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

        public void AddUser(User user)
        {
            generator.AddUser(user);
        }

        public void AddOrder(Order order)
        {
            generator.AddOrder(order);
        }

        public void AddProjectMembers(params ProjectMember[] projectMembers)
        {
            generator.AddProjectMembers(projectMembers);
        }

        public void AddProjects(params Project[] projects)
        {
            generator.AddProjects(projects);
        }

        public void AddOrUpdateProject(Project project)
        {
            generator.AddOrUpdateProject(project);
        }

        public void AddOrUpdateProjectMember(ProjectMember projectMember)
        {
            generator.AddOrUpdateProjectMember(projectMember);
        }

        public void AddOrUpdateGraphNode(Entities.GraphNode graphNode)
        {
            generator.AddOrUpdateGraphNode(graphNode);
        }

        public void DeleteGraphNode(Entities.GraphNode graphNode)
        {
            generator.DeleteGraphNode(graphNode);
        }

        public void GenerateTpcDi(int securityCount, int dailyMarketDays)
        {
            generator.GenerateTpcDi(securityCount, dailyMarketDays);
        }

        public void GenerateDailyMarkets(int dailyMarketDays)
        {
            generator.GenerateDailyMarkets(dailyMarketDays);
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

        public Task CreateStream(
            string sql,
            int parallelism = 1,
            StateSerializeOptions? stateSerializeOptions = default,
            TimeSpan? timestampInterval = default,
            int pageSize = 1024,
            bool ignoreSameDataCheck = false,
            ICheckFailureListener? checkFailureListener = default,
            PlanOptimizerSettings? planOptimizerSettings = default,
            string? version = default,
            DistributedOptions? distributedOptions = default)
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

            plan = Core.Optimizer.PlanOptimizer.Optimize(plan, planOptimizerSettings);

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
                    DefaultBPlusTreePageSizeBytes = BPlusTreePageSizeBytes,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{testName}/tmp"
                    }
                });

            if (!string.IsNullOrWhiteSpace(version))
            {
                flowtideBuilder.SetVersion(version);
            }

            if (distributedOptions != null)
            {
                flowtideBuilder.SetDistributedOptions(distributedOptions);
            }

            if (checkFailureListener != null)
            {
                flowtideBuilder.WithCheckFailureListener(checkFailureListener);
            }

            if (WaitForCheckpointAfterInitialData)
            {
                flowtideBuilder.WaitForCheckpointAfterInitialData(true);
            }

            if (MinimumTimeBetweenCheckpoints.HasValue)
            {
                flowtideBuilder.SetMinimumTimeBetweenCheckpoint(MinimumTimeBetweenCheckpoints.Value);
            }

            if (StopDrainTimeout.HasValue)
            {
                flowtideBuilder.SetStopDrainTimeout(StopDrainTimeout.Value);
            }

            var stream = flowtideBuilder.Build();
            _stream = stream;
            return Task.CompletedTask;
        }

        public async Task StartStream(
            string sql,
            int parallelism = 1,
            StateSerializeOptions? stateSerializeOptions = default,
            TimeSpan? timestampInterval = default,
            int pageSize = 1024,
            bool ignoreSameDataCheck = false,
            ICheckFailureListener? checkFailureListener = default,
            PlanOptimizerSettings? planOptimizerSettings = default,
            string? version = default,
            DistributedOptions? distributedOptions = default)
        {
            await CreateStream(sql, parallelism, stateSerializeOptions, timestampInterval, pageSize, ignoreSameDataCheck, checkFailureListener, planOptimizerSettings, version, distributedOptions);
            await _stream!.StartAsync();
        }

        protected virtual IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            return new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = new MemoryFileProvider()});
        }

        private void OnDataUpdate(EventBatchData actualData)
        {
            lock (_lock)
            {
                _actualData = actualData;
                _dataUpdated = true;
            }
        }

        private bool _waitForUpdateDoesNotRequireDataChange = false;

        public void WaitForUpdateDoesNotRequireDataChange()
        {
            _waitForUpdateDoesNotRequireDataChange = true;
        }

        private void CheckpointComplete()
        {
            lock (_lock)
            {
                if (_dataUpdated || _waitForUpdateDoesNotRequireDataChange)
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
            while (_stream.State == StreamStateValue.Running && graph.State != StreamStateValue.Failure)
            {
                graph = _stream.GetDiagnosticsGraph();
                await scheduler!.Tick();
                await Task.Delay(TimeSpan.FromMilliseconds(10));
                CheckForErrors();
            }
        }

        /// <summary>
        /// Fires the crash trigger without waiting for the stream to reach the failure
        /// state, so a test can control scheduler ticks around the crash itself.
        /// </summary>
        public Task FireCrashTrigger()
        {
            return _stream!.CallTrigger("crash", default);
        }

        public async Task StopMockIngressAutocompleteDependencies()
        {
            await _stream!.CallTrigger("ingress_no_autocomplete_dependencies", default);
        }

        public async Task MockIngressFailAndRollback(long restoreVersion)
        {
            await _stream!.CallTrigger("ingress_fail_and_rollback", restoreVersion);
        }

        public async Task MockIngressSetDependenciesDone()
        {
            await _stream!.CallTrigger("ingress_dependencies_done", default);
        }

        public async Task HealthyFor(TimeSpan time)
        {
            Debug.Assert(_stream != null);
            var graph = _stream.GetDiagnosticsGraph();
            var scheduler = _stream.Scheduler as DefaultStreamScheduler;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            while (_stream.State == StreamStateValue.Running && graph.State != StreamStateValue.Failure)
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
            if (graph.State != StreamStateValue.Running || graph.State != StreamStateValue.Running)
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

        /// <summary>
        /// When true, stream failures without an exception do not fail the test.
        /// Used by tests that expect a fail and recover, for example distributed tests
        /// where substreams recover to a common checkpoint version.
        /// </summary>
        public bool AllowFailureAndRecover { get; set; }

        private void CheckForErrors()
        {
            if (_notificationReciever != null && _notificationReciever._error)
            {
                if (_notificationReciever._exception != null)
                {
                    throw _notificationReciever._exception;
                }
                else if (!AllowFailureAndRecover)
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

        private bool _immutableSource = false;

        public void SourceImmutable()
        {
            _immutableSource = true;
        }

        protected virtual void AddReadResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddSource(new MockSourceFactory("*", _db, _immutableSource, InitialDataDelay));
        }

        /// <summary>
        /// Makes the sinks DeleteAsync throw this many times, simulating a storage delete
        /// that fails transiently, or permanently when set above the delete retry budget.
        /// </summary>
        public int SinkDeleteFailCount { get; set; }

        protected virtual void AddWriteResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddSink(new MockSinkFactory("*", OnDataUpdate, _egressCrashOnCheckpointCount, OnWatermark, deleteFailCount: SinkDeleteFailCount));
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

        public async Task Trigger(string triggerName, object? state = default)
        {
            await _stream!.CallTrigger(triggerName, state);
        }

        public StreamGraph GetDiagnosticsGraph()
        {
            return _stream!.GetDiagnosticsGraph();
        }

        public void Pause()
        {
            _stream!.Pause();
        }

        public void Resume()
        {
            _stream!.Resume();
        }

        public Task StopStream()
        {
            return _stream!.StopAsync();
        }

        /// <summary>
        /// Injects a failure as if a block had faulted, so a test can drive the failure
        /// paths at a precise moment, for example while a state manager write is held in
        /// flight by a test hook.
        /// </summary>
        public Task InjectFailure(Exception exception)
        {
            return _stream!.InjectFailureForTests(exception);
        }

        /// <summary>
        /// Delivers an egress checkpoint done into the stream as if an egress vertex fired it,
        /// so a test can reproduce a spurious or stale acknowledgement arriving at a precise
        /// moment.
        /// </summary>
        public void InjectEgressCheckpointDone(string operatorName, ILockingEvent? lockingEvent)
        {
            _stream!.InjectEgressCheckpointDoneForTests(operatorName, lockingEvent);
        }

        public Task StartStream()
        {
            return _stream!.StartAsync();
        }

        public Task DeleteStream()
        {
            return _stream!.DeleteAsync();
        }
    }
}
