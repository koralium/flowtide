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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar.Functions.CheckFunctions;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.CheckExtraction;
using FlowtideDotNet.Engine.FailureStrategies;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Core.Engine
{
    public class FlowtideBuilder
    {
        DataflowStreamBuilder dataflowStreamBuilder;
        private Plan? _plan;
        private IConnectorManager? _connectorManager;
        private IReadWriteFactory? _readWriteFactory;
        private IStateHandler? _stateHandler;
        private int _queueSize = 100;
        private FunctionsRegister _functionsRegister;
        private int _parallelism = 1;
        private TimeSpan _getTimestampInterval = TimeSpan.FromHours(1);
        private TaskScheduler? _taskScheduler;
        private bool _useColumnStore = true;
        private LateMaterializationMode _lateMaterialization = LateMaterializationMode.Auto;
        // Read at build, the options may change until then
        private StateManagerOptions? _stateManagerOptions;
        private int _cachePageCount => _stateManagerOptions?.CachePageCount ?? new StateManagerOptions().CachePageCount;
        // Without options the stream's storage is the file cache
        private bool _storageHoldsRowStores => _stateManagerOptions?.PersistentStorage is global::FlowtideDotNet.Storage.Persistence.Reservoir.Internal.ReservoirPersistentStorage;
        // False where page reads share one lock, not qualified yet
        private bool _storageReadsSideBySide => _stateManagerOptions?.PersistentStorage is global::FlowtideDotNet.Storage.Persistence.Reservoir.Internal.ReservoirPersistentStorage reservoir && reservoir.ReadsPagesWithoutFileLock;
        // False when the plan could not be serialized
        private bool _planSerializes = true;

        // Pages per store's write queue, under one: no fit.
        // A store holds two pages besides its queue, a reader one.
        private int WriteQueuePages(int stores, int readers)
        {
            return RowStoreWriteQueuePages(_cachePageCount, stores, readers);
        }

        // The rewrite cannot be undone, so it is tried on a copy of the plan.
        // Null when the copy's stores can be hashed and fit the cache.
        private string? WhyNotPlaced(LateMaterialization.LateMaterializationReport report)
        {
            Debug.Assert(_plan != null);
            Plan copy;
            LateMaterialization.LateMaterializationReport decided;
            try
            {
                // Connectors may have changed the plan since its first hash
                copy = new SubstraitDeserializer().Deserialize(SubstraitSerializer.Serialize(_plan));
                decided = LateMaterialization.LateMaterializationPass.Analyze(copy, _connectorManager, _functionsRegister, _parallelism);
            }
            catch (Exception e)
            {
                return "the plan cannot be copied through its serialized form: " + e.Message;
            }
            // The copy stands for the plan only when it is decided the same
            if (!decided.Hosts.Select(x => (x.Table, x.Admitted, string.Join(',', x.Columns))).SequenceEqual(report.Hosts.Select(x => (x.Table, x.Admitted, string.Join(',', x.Columns)))))
            {
                return "the plan reads back differently from its serialized form";
            }
            try
            {
                LateMaterialization.LateMaterializationPass.Run(copy, _connectorManager, _functionsRegister, _parallelism, decided.Selection());
                SubstraitSerializer.SerializeToJson(copy, includeHints: false);
            }
            catch (Exception e)
            {
                return "the plan with its stores cannot be serialized for its version: " + e.Message;
            }
            LateMaterialization.RowStoreHostCollector.Count(copy.Relations, out int stores, out int readers);
            if (stores == 0 || WriteQueuePages(stores, readers) < 1)
            {
                return $"the cache of {_cachePageCount} pages cannot hold the pages of {stores} row stores and their {readers} readers";
            }
            return null;
        }

        // The cache lets one page in twenty be held outside eviction
        internal static int RowStoreWriteQueuePages(int cachePages, int stores, int readers)
        {
            return Math.Min(16, (cachePages / 20 - readers) / stores - 2);
        }

        // What the planner decided at the last build, for tests
        internal LateMaterialization.LateMaterializationReport? LateMaterializationReport { get; private set; }
        private string _version = "";
        private List<(string? stringVersion, bool? addHashVersion)>? _versionParts;
        private bool _isCheckListenerRegistered = false;
        private readonly string _streamName;
        private OpenLineageHttpOptions? _openLineageHttpOptions;
        private readonly List<IStreamLineageListener> _lineageListeners = new List<IStreamLineageListener>();
        private DistributedOptions? _distributedOptions;

        public FlowtideBuilder(string streamName)
        {
            dataflowStreamBuilder = new DataflowStreamBuilder(streamName);
            _functionsRegister = new FunctionsRegister();
            // Register default functions directly
            BuiltinFunctions.RegisterFunctions(_functionsRegister);
            this._streamName = streamName;
        }

        public IFunctionsRegister FunctionsRegister => _functionsRegister;

        public FlowtideBuilder AddPlan(Plan plan, bool optimize = true, PlanOptimizerSettings? planOptimizerSettings = default)
        {
            if (optimize)
            {
                plan = Optimizer.PlanOptimizer.Optimize(plan, planOptimizerSettings);
            }

            _plan = plan;
            return this;
        }

        public FlowtideBuilder AddConnectorManager(IConnectorManager connectorManager)
        {
            _connectorManager = connectorManager;
            return this;
        }

        [Obsolete("Use ConnectorManager instead")]
        public FlowtideBuilder AddReadWriteFactory(IReadWriteFactory readWriteFactory)
        {
            _readWriteFactory = readWriteFactory;
            return this;
        }

        public FlowtideBuilder WithStateOptions(StateManagerOptions stateManagerOptions)
        {
            _stateManagerOptions = stateManagerOptions;
            dataflowStreamBuilder.WithStateOptions(stateManagerOptions);
            return this;
        }

        public FlowtideBuilder AddStateHandler(IStateHandler stateHandler)
        {
            dataflowStreamBuilder.WithStateHandler(stateHandler);
            _stateHandler = stateHandler;
            return this;
        }

        public FlowtideBuilder WithScheduler(IStreamScheduler streamScheduler)
        {
            dataflowStreamBuilder.WithStreamScheduler(streamScheduler);
            return this;
        }

        public FlowtideBuilder WithCheckpointListener(ICheckpointListener listener)
        {
            dataflowStreamBuilder.AddCheckpointListener(listener);
            return this;
        }

        public FlowtideBuilder WithStateChangeListener(IStreamStateChangeListener listener)
        {
            dataflowStreamBuilder.AddStateChangeListener(listener);
            return this;
        }

        public FlowtideBuilder WithCheckpointListener<T>()
            where T : ICheckpointListener, new()
        {
            dataflowStreamBuilder.AddCheckpointListener(new T());
            return this;
        }

        public FlowtideBuilder WithStateChangeListener<T>()
            where T : IStreamStateChangeListener, new()
        {
            dataflowStreamBuilder.AddStateChangeListener(new T());
            return this;
        }

        public FlowtideBuilder WithFailureListener(IFailureListener listener)
        {
            dataflowStreamBuilder.AddFailureListener(listener);
            return this;
        }

        public FlowtideBuilder WithCheckFailureListener(ICheckFailureListener listener)
        {
            _isCheckListenerRegistered = true;
            dataflowStreamBuilder.AddCheckFailureListener(listener);
            return this;
        }

        /// <summary>
        /// Reports the state and counts of every check at each start and when they change.
        /// </summary>
        public FlowtideBuilder WithCheckStatusListener(ICheckStatusListener listener)
        {
            _isCheckListenerRegistered = true;
            dataflowStreamBuilder.AddCheckStatusListener(listener);
            return this;
        }

        public FlowtideBuilder WithFailureListener<T>()
            where T : IFailureListener, new()
        {
            dataflowStreamBuilder.AddFailureListener(new T());
            return this;
        }

        public FlowtideBuilder WithFailureListener(Action<Exception?> exceptionAction)
        {
            dataflowStreamBuilder.AddFailureListener(new CustomExceptionStrategy(exceptionAction));
            return this;
        }

        public FlowtideBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            dataflowStreamBuilder.WithLoggerFactory(loggerFactory);
            return this;
        }

        public FlowtideBuilder WithPauseMonitor(IOptionsMonitor<FlowtidePauseOptions> pauseMonitor)
        {
            dataflowStreamBuilder.WithPauseMonitor(pauseMonitor);
            return this;
        }

        public FlowtideBuilder SetMessageQueueSize(int queueSize)
        {
            _queueSize = queueSize;
            return this;
        }

        public FlowtideBuilder SetParallelism(int parallelism)
        {
            _parallelism = parallelism;
            return this;
        }

        public FlowtideBuilder SetGetTimestampUpdateInterval(TimeSpan interval)
        {
            _getTimestampInterval = interval;
            return this;
        }

        /// <summary>
        /// Throttles checkpoints so a chatty source cannot storm.
        /// </summary>
        /// <remarks>
        /// The first checkpoint per start and all drains bypass it.
        /// </remarks>
        /// <param name="timeSpan">The minimum interval between throttled checkpoints.</param>
        public FlowtideBuilder SetMinimumTimeBetweenCheckpoint(TimeSpan timeSpan)
        {
            dataflowStreamBuilder.SetMinimumTimeBetweenCheckpoint(timeSpan);
            return this;
        }

        /// <summary>
        /// Sets how long a stopping stream waits for vertices that exchange data with other
        /// substreams to drain before it begins teardown. Active callbacks and storage
        /// operations must still settle before their resources are released.
        /// </summary>
        public FlowtideBuilder SetStopDrainTimeout(TimeSpan timeSpan)
        {
            dataflowStreamBuilder.SetStopDrainTimeout(timeSpan);
            return this;
        }

        /// <summary>
        /// When enabled the stream takes a checkpoint right after its initial data has been
        /// loaded, holding off any other checkpoint until then.
        /// </summary>
        public FlowtideBuilder WaitForCheckpointAfterInitialData(bool wait)
        {
            dataflowStreamBuilder.WaitForCheckpointAfterInitialData(wait);
            return this;
        }

        public FlowtideBuilder SetTaskScheduler(TaskScheduler taskScheduler)
        {
            _taskScheduler = taskScheduler;
            return this;
        }

        public FlowtideBuilder ColumnStore(bool use)
        {
            _useColumnStore = use;
            return this;
        }

        /// <summary>
        /// Sets how the stream places row stores for wide columns.
        /// Read at <see cref="Build"/>, so it covers plans added without optimization.
        /// </summary>
        public FlowtideBuilder SetLateMaterialization(LateMaterializationMode mode)
        {
            _lateMaterialization = mode;
            return this;
        }

        public FlowtideBuilder SetVersion(string version)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(version);
            _version = version;
            return this;
        }

        public FlowtideBuilder SetVersionParts(IEnumerable<(string?, bool?)> versionParts)
        {
            _versionParts = [.. versionParts];
            return this;
        }

        public FlowtideBuilder WithCheckLogger(LogLevel logLevel = LogLevel.Warning)
        {
            if (dataflowStreamBuilder.LoggerFactory == null)
            {
                throw new InvalidOperationException("LoggerFactory is not set. Cannot add check logger.");
            }
            var listener = new LoggerCheckFailureListener(dataflowStreamBuilder.LoggerFactory.CreateLogger<LoggerCheckFailureListener>(), logLevel);
            WithCheckFailureListener(listener);
            WithCheckStatusListener(listener);
            return this;
        }

        /// <summary>
        /// Reports check failures and resolutions as CheckFailure and CheckResolved activities on the FlowtideDotNet.CheckFailures source.
        /// </summary>
        /// <returns></returns>
        public FlowtideBuilder WithCheckActivityLogger()
        {
            WithCheckFailureListener(new ActivityCheckFailureListener());
            return this;
        }

        public FlowtideBuilder WithOpenLineageHttp(OpenLineageHttpOptions options)
        {
            _openLineageHttpOptions = options;
            return this;
        }

        // Lineage packages hook in here, the listener gets the lineage after a successful build.
        internal FlowtideBuilder AddLineageListener(IStreamLineageListener listener)
        {
            ArgumentNullException.ThrowIfNull(listener);
            _lineageListeners.Add(listener);
            return this;
        }

        // Roles are state layouts the plan's text does not show
        private string ComputePlanHash(string roles = "")
        {
            Debug.Assert(_plan != null, "Plan should not be null.");
            using (SHA256 sha256 = SHA256.Create())
            {
                string json = "";
                try
                {
                    // Hints are left out of the plan hash
                    json = SubstraitSerializer.SerializeToJson(_plan, includeHints: false);
                }
                catch (Exception e)
                {
                    if (Operators.Join.MultiJoin.MultiJoinMarker.Count(_plan) > 0)
                    {
                        // Without a hash the stream could not tell that its joins are stored in another way than before
                        throw new InvalidOperationException("The plan has a multi join and could not be serialized, so its version cannot be told. Turn MultiJoin off for this plan.", e);
                    }
                    bool rowStores;
                    try
                    {
                        rowStores = LateMaterialization.RowStoreHostCollector.UsesRowStores(_plan.Relations);
                    }
                    catch (InvalidOperationException)
                    {
                        // A store named twice, still a plan with stores
                        rowStores = true;
                    }
                    if (rowStores)
                    {
                        // Without a hash a changed store layout goes unseen
                        throw new InvalidOperationException("The plan has a row store and could not be serialized, so its version cannot be told.", e);
                    }
                    _planSerializes = false;
                    Console.Error.WriteLine("Failed to serialize plan for hash check.");
                }
                var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(json + roles));
                return Base62.Encode(hashBytes);
            }
        }

        public FlowtideBuilder SetDistributedOptions(DistributedOptions distributedOptions)
        {
            _distributedOptions = distributedOptions;
            return this;
        }


        // The reporter of the last build.
        internal OpenLineageHttpReporter? OpenLineageReporterForTests { get; private set; }

        public FlowtideDotNet.Base.Engine.DataflowStream Build()
        {
            if (_plan == null)
            {
                throw new InvalidOperationException("No plan has been added.");
            }
            if (_connectorManager == null && _readWriteFactory == null)
            {
                throw new InvalidOperationException("No connector manager or ReadWriteFactory has been added.");
            }

            // Safety net for plans added without optimization
            _plan = CheckExtractor.Extract(_plan);

            _planSerializes = true;
            var hash = ComputePlanHash();

            // Modify plan
            if (_connectorManager != null)
            {
                var planModifier = new ConnectorPlanModifyVisitor(_connectorManager);
                planModifier.VisitPlan(_plan);
            }

            if (_lateMaterialization == LateMaterializationMode.ForceOn && _useColumnStore)
            {
                LateMaterialization.LateMaterializationPass.Run(_plan, _connectorManager, _functionsRegister, _parallelism);
            }
            else if (_lateMaterialization != LateMaterializationMode.Off && _useColumnStore)
            {
                // Placebo decides and places nothing
                LateMaterialization.LateMaterializationReport report;
                try
                {
                    report = LateMaterialization.LateMaterializationPass.Analyze(_plan, _connectorManager, _functionsRegister, _parallelism);
                }
                catch (Exception e)
                {
                    // A plan it cannot read runs without stores
                    report = new LateMaterialization.LateMaterializationReport();
                    report.Skip("the plan could not be analysed: " + e.Message);
                }
                LateMaterializationReport = report;
                // Auto never fails a stream that runs without stores
                if (!_storageHoldsRowStores)
                {
                    report.Skip("the state storage cannot hold row stores, they need Reservoir storage");
                }
                else if (!_storageReadsSideBySide)
                {
                    report.Skip("the state storage reads one page at a time, row stores are not qualified on it yet");
                }
                else if (!_planSerializes)
                {
                    // A plan with stores must have a hash of its text
                    report.Skip("the plan cannot be serialized, so its version could not follow its stores");
                }
                else if (report.Admitted > 0 && WhyNotPlaced(report) is string reason)
                {
                    report.Skip(reason);
                }
                if (report.Hosts.Count > 0)
                {
                    dataflowStreamBuilder.LoggerFactory?.CreateLogger("FlowtideDotNet.Core.LateMaterialization").LogInformation("Late materialization ({Mode}) for stream {Stream}: {Report}", _lateMaterialization, _streamName, report.ToString());
                }
                if (_lateMaterialization == LateMaterializationMode.Auto && report.Admitted > 0 && report.Skipped.Length == 0)
                {
                    LateMaterialization.LateMaterializationPass.Run(_plan, _connectorManager, _functionsRegister, _parallelism, report.Selection());
                }
            }
            if (LateMaterialization.RowStoreHostCollector.UsesRowStores(_plan.Relations))
            {
                // The version follows the final plan and its sinks' roles
                hash = ComputePlanHash(LateMaterialization.RowStoreRoles.Describe(_plan, _connectorManager));
            }

            if (_versionParts?.Count > 0)
            {
                StringBuilder sb = new();
                foreach (var (stringVersion, addHashVersion) in _versionParts)
                {
                    string? part = null;
                    if (!string.IsNullOrEmpty(stringVersion))
                    {
                        part = stringVersion;
                    }
                    else if (addHashVersion ?? false)
                    {
                        part = hash;
                    }

                    if (sb.Length > 0)
                    {
                        sb.Append('-');
                    }

                    sb.Append(part);
                }

                SetVersion(sb.ToString());
            }

            dataflowStreamBuilder.SetVersionInformation(hash, _version);

            if (_distributedOptions != null)
            {
                var substreamNames = _plan.Relations.OfType<SubStreamRootRelation>().Select(x => x.Name).Distinct().ToList();
                if (substreamNames.Count > 0 && !substreamNames.Contains(_distributedOptions.SubstreamName))
                {
                    // Every substream root would be skipped, silently building an empty
                    // stream that produces nothing while the other substreams wait for it.
                    throw new InvalidOperationException(
                        $"The substream name '{_distributedOptions.SubstreamName}' does not exist in the plan, it contains the substreams: {string.Join(", ", substreamNames)}.");
                }
            }

            // A row reference only goes where it can be taken
            LateMaterialization.RowReferencePlanValidator.Validate(_plan.Relations, _functionsRegister, _parallelism, _useColumnStore);

            SubstraitVisitor visitor = new SubstraitVisitor(
                _plan,
                dataflowStreamBuilder,
                _connectorManager,
                _readWriteFactory,
                _queueSize,
                _functionsRegister,
                _parallelism,
                _getTimestampInterval,
                _useColumnStore,
                dataflowStreamBuilder.LoggerFactory,
                _taskScheduler,
                _distributedOptions);
            LateMaterialization.RowStoreHostCollector.Count(_plan.Relations, out int rowStores, out int rowStoreReaders);
            if (rowStores > 0)
            {
                // The cache sizes the stores, it does not choose them
                visitor.RowStoreWriteQueuePages = WriteQueuePages(rowStores, rowStoreReaders);
                if (visitor.RowStoreWriteQueuePages < 1)
                {
                    throw new InvalidOperationException(
                        $"The plan has {rowStores} row stores and {rowStoreReaders} readers of them, a cache of {_cachePageCount} pages cannot hold their pages. Set CachePageCount to {(rowStores * 3 + rowStoreReaders) * 20} or more.");
                }
            }

            StreamLineage? lineage = null;
            OpenLineageHttpReporter? reporter = null;
            ILogger lineageLogger = NullLogger.Instance;
            if (_openLineageHttpOptions != null || _lineageListeners.Count > 0)
            {
                lineageLogger = dataflowStreamBuilder.LoggerFactory?.CreateLogger("FlowtideDotNet.Core.Lineage") ?? NullLogger.Instance;
                if (_connectorManager == null)
                {
                    lineageLogger.LogWarning("Lineage requires a connector manager, lineage is disabled for stream '{StreamName}'.", _streamName);
                }
                else
                {
                    // A missing Url still fails the build.
                    if (_openLineageHttpOptions != null)
                    {
                        OpenLineageHttpReporter.ValidateOptions(_openLineageHttpOptions);
                    }
                    try
                    {
                        lineage = StreamLineageExtractor.Extract(new StreamLineageExtractionContext()
                        {
                            Plan = _plan,
                            ConnectorManager = _connectorManager,
                            BuilderStreamName = _streamName,
                            SubstreamScope = _distributedOptions?.SubstreamName,
                            IncludeConnectorSchema = (_openLineageHttpOptions?.IncludeSchema ?? false) || _lineageListeners.Any(x => x.IncludeConnectorSchema)
                        });
                        if (_openLineageHttpOptions != null)
                        {
                            reporter = OpenLineageHttpReporter.Create(dataflowStreamBuilder.LoggerFactory, lineage, _openLineageHttpOptions);
                            WithStateChangeListener(reporter);
                            // Stopped only when the stream is disposed.
                            dataflowStreamBuilder.AddOwnedResource(reporter);
                            OpenLineageReporterForTests = reporter;
                        }
                    }
                    catch (Exception ex)
                    {
                        // Lineage never stops the stream from building.
                        lineage = null;
                        lineageLogger.LogError(ex, "Failed to extract lineage for stream '{StreamName}', lineage is disabled for this build.", _streamName);
                    }
                }
            }

            if (!_isCheckListenerRegistered)
            {
                // This should perhaps be moved in the future to some validation step
                if (CheckFunctionFinder.CheckPlan(_plan) &&
                    dataflowStreamBuilder.LoggerFactory != null)
                {
                    var checkFunctionLogger = dataflowStreamBuilder.LoggerFactory.CreateLogger("FlowtideDotNet.Core.Engine.CheckFunctionFinder");
                    checkFunctionLogger.LogWarning("Check function found in plan, but no check failure or check status listener is registered.");
                }
            }

            FlowtideDotNet.Base.Engine.DataflowStream stream;
            try
            {
                visitor.BuildPlan();
                stream = dataflowStreamBuilder.Build();
            }
            catch
            {
                // No stream owns the reporter, nothing is queued.
                _ = reporter?.DisposeAsync();
                throw;
            }

            // Only a successful build reaches the listeners.
            if (lineage != null && _lineageListeners.Count > 0)
            {
                try
                {
                    lineage = lineage.WithChecks(StreamLineageCheckExtractor.Extract(_plan, _connectorManager!, lineage, visitor.BuiltChecks, _distributedOptions != null));
                }
                catch (Exception ex)
                {
                    lineageLogger.LogError(ex, "Failed to extract the checks of stream '{StreamName}', the lineage has no checks for this build.", _streamName);
                }
                var logicalStreamName = LineageStreamNames.GetLogicalStreamName(_streamName, _distributedOptions?.SubstreamName);
                foreach (var listener in _lineageListeners)
                {
                    try
                    {
                        listener.OnStreamBuilt(lineage, logicalStreamName);
                    }
                    catch (Exception ex)
                    {
                        lineageLogger.LogError(ex, "Lineage listener {Listener} failed for stream '{StreamName}'.", listener.GetType().Name, _streamName);
                    }
                }
            }

            return stream;
        }
    }
}
