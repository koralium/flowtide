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

using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FlowtideDotNet.Base.Engine
{
    /// <summary>
    /// A fluent builder for configuring and constructing a <see cref="DataflowStream"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="DataflowStreamBuilder"/> assembles all components required to run a dataflow stream:
    /// ingress vertices (sources), propagator vertices (transforms), and egress vertices (sinks),
    /// together with state management, scheduling, logging, versioning, and notification callbacks.
    /// Each configuration method returns the same builder instance to support method chaining.
    /// </para>
    /// <para>
    /// The only required configuration before calling <see cref="Build"/> is
    /// <see cref="WithStateOptions"/>, which provides the state manager persistence settings.
    /// All other configuration is optional and falls back to sensible defaults:
    /// <list type="bullet">
    ///   <item><description><see cref="WithStreamScheduler"/> defaults to <see cref="DefaultStreamScheduler"/>.</description></item>
    ///   <item><description><see cref="WithStateHandler"/> defaults to a no-op <c>NullStateHandler</c>.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// For higher-level SQL-plan-based stream construction, prefer <c>FlowtideBuilder</c> from
    /// <c>FlowtideDotNet.Core</c>, which wraps this builder and handles plan compilation, operator
    /// wiring, and hash-based version tracking automatically.
    /// </para>
    /// </remarks>
    public class DataflowStreamBuilder
    {
        private readonly Dictionary<string, IStreamVertex> _propagatorBlocks = new Dictionary<string, IStreamVertex>();
        private readonly Dictionary<string, IStreamIngressVertex> _ingressBlocks = new Dictionary<string, IStreamIngressVertex>();
        private readonly Dictionary<string, IStreamEgressVertex> _egressBlocks = new Dictionary<string, IStreamEgressVertex>();
        private StreamState? _state;
        private IStateHandler? _stateHandler;
        private readonly string _streamName;
        private string _version = "";
        private IStreamScheduler? _streamScheduler;
        private StateManagerOptions? _stateManagerOptions;
        private ILoggerFactory? _loggerFactory;
        private StreamVersionInformation? _streamVersionInformation;
        private readonly DataflowStreamOptions _dataflowStreamOptions;
        private IOptionsMonitor<FlowtidePauseOptions>? _pauseMonitor;
        private readonly StreamNotificationReceiver _streamNotificationReceiver;

        internal StreamNotificationReceiver StreamNotificationReceiver => _streamNotificationReceiver;
        internal ILoggerFactory? LoggerFactory => _loggerFactory;

        /// <summary>
        /// Initializes a new instance of <see cref="DataflowStreamBuilder"/> for the stream with the given name.
        /// </summary>
        /// <param name="streamName">
        /// The unique name of the stream. Used to namespace state storage, metrics, and log output
        /// throughout the lifetime of the stream.
        /// </param>
        public DataflowStreamBuilder(string streamName)
        {
            _streamName = streamName;
            _dataflowStreamOptions = new DataflowStreamOptions();
            _streamNotificationReceiver = new StreamNotificationReceiver(streamName);
        }

        /// <summary>
        /// Registers a propagator (transform) vertex under the given operator name.
        /// </summary>
        /// <remarks>
        /// Propagator vertices sit between ingress and egress vertices, transforming or routing stream events.
        /// The <paramref name="name"/> is used as the operator's unique identity for state storage,
        /// metrics, and inter-vertex linking. <see cref="IStreamVertex.Setup"/> is called on the block
        /// immediately so it is ready for linking.
        /// </remarks>
        /// <param name="name">The unique operator name for this vertex within the stream.</param>
        /// <param name="block">The propagator vertex to register.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder AddPropagatorBlock(string name, IStreamVertex block)
        {
            block.Setup(_streamName, name);
            _propagatorBlocks.Add(name, block);
            return this;
        }

        /// <summary>
        /// Registers an ingress (source) vertex under the given operator name.
        /// </summary>
        /// <remarks>
        /// Ingress vertices are the entry points of the stream, producing <see cref="IStreamEvent"/> messages
        /// from external data sources. <see cref="IStreamVertex.Setup"/> is called on the block immediately.
        /// </remarks>
        /// <param name="name">The unique operator name for this vertex within the stream.</param>
        /// <param name="block">The ingress vertex to register.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder AddIngressBlock(string name, IStreamIngressVertex block)
        {
            block.Setup(_streamName, name);
            _ingressBlocks.Add(name, block);
            return this;
        }

        /// <summary>
        /// Registers an egress (sink) vertex under the given operator name.
        /// </summary>
        /// <remarks>
        /// Egress vertices are the terminal points of the stream, consuming <see cref="IStreamEvent"/> messages
        /// and writing results to external systems. <see cref="IStreamVertex.Setup"/> is called on the block immediately.
        /// </remarks>
        /// <param name="name">The unique operator name for this vertex within the stream.</param>
        /// <param name="block">The egress vertex to register.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder AddEgressBlock(string name, IStreamEgressVertex block)
        {
            block.Setup(_streamName, name);
            _egressBlocks.Add(name, block);
            return this;
        }

        /// <summary>
        /// Seeds the builder with a previously persisted <see cref="StreamState"/> to restore the stream
        /// from a known logical time rather than starting fresh.
        /// </summary>
        /// <param name="state">The stream state to restore from.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder FromState(StreamState state)
        {
            _state = state;
            return this;
        }

        /// <summary>
        /// Replaces the default <see cref="DefaultStreamScheduler"/> with a custom <see cref="IStreamScheduler"/>
        /// implementation for managing trigger scheduling.
        /// </summary>
        /// <param name="streamScheduler">The custom scheduler to use.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder WithStreamScheduler(IStreamScheduler streamScheduler)
        {
            _streamScheduler = streamScheduler;
            return this;
        }

        /// <summary>
        /// Sets the <see cref="IStateHandler"/> responsible for persisting and loading top-level stream state
        /// snapshots. Defaults to a no-op handler if not specified.
        /// </summary>
        /// <param name="stateHandler">The state handler to use.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder WithStateHandler(IStateHandler stateHandler)
        {
            _stateHandler = stateHandler;
            return this;
        }

        /// <summary>
        /// Sets the <see cref="StateManagerOptions"/> that configure how operator state is persisted
        /// and managed. This is the only required configuration before calling <see cref="Build"/>.
        /// </summary>
        /// <param name="stateManagerOptions">The persistence options for the state manager.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder WithStateOptions(StateManagerOptions stateManagerOptions)
        {
            _stateManagerOptions = stateManagerOptions;
            return this;
        }

        /// <summary>
        /// Sets the <see cref="ILoggerFactory"/> used to create loggers for all vertices and
        /// internal stream components.
        /// </summary>
        /// <param name="loggerFactory">The logger factory to use.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            return this;
        }

        /// <summary>
        /// Sets the stream's plan hash and human-readable version string, used to detect structural changes
        /// between stream restarts and guard against incompatible state restores.
        /// </summary>
        /// <remarks>
        /// When set, the stream engine compares <paramref name="hash"/> against the hash stored in persisted
        /// state on startup. A mismatch causes an <see cref="InvalidOperationException"/> to be thrown,
        /// preventing accidental state corruption when the stream topology has changed.
        /// </remarks>
        /// <param name="hash">A content hash of the stream plan, used for structural change detection.</param>
        /// <param name="version">A human-readable version label associated with this stream configuration.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder SetVersionInformation(string hash, string version)
        {
            _streamVersionInformation = new StreamVersionInformation(hash, version);
            return this;
        }

        /// <summary>
        /// Controls whether the stream should wait for a checkpoint to complete after all ingress vertices
        /// have finished emitting their initial data load.
        /// </summary>
        /// <remarks>
        /// When <see langword="true"/> (the default), the stream takes a checkpoint immediately after
        /// initial data is fully propagated, establishing a clean recovery baseline before transitioning
        /// to incremental processing.
        /// </remarks>
        /// <param name="wait">
        /// <see langword="true"/> to wait for a checkpoint after initial data; <see langword="false"/> to proceed immediately.
        /// </param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder WaitForCheckpointAfterInitialData(bool wait)
        {
            _dataflowStreamOptions.WaitForCheckpointAfterInitialData = wait;
            return this;
        }

        /// <summary>
        /// Sets the minimum time that must elapse between two consecutive checkpoint triggers,
        /// preventing excessive checkpointing under high-throughput conditions.
        /// </summary>
        /// <param name="timeSpan">The minimum interval between checkpoints.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder SetMinimumTimeBetweenCheckpoint(TimeSpan timeSpan)
        {
            _dataflowStreamOptions.MinimumTimeBetweenCheckpoints = timeSpan;
            return this;
        }

        /// <summary>
        /// Subscribes an <see cref="ICheckpointListener"/> to receive a notification each time
        /// the stream completes a checkpoint boundary.
        /// </summary>
        /// <remarks>
        /// Multiple listeners can be registered. Exceptions thrown by a listener are swallowed to
        /// ensure they cannot disrupt the stream.
        /// </remarks>
        /// <param name="listener">The listener to register.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder AddCheckpointListener(ICheckpointListener listener)
        {
            _streamNotificationReceiver.AddCheckpointListener(listener);
            return this;
        }

        /// <summary>
        /// Subscribes an <see cref="IStreamStateChangeListener"/> to receive a notification each time
        /// the stream transitions to a new <see cref="StreamStateValue"/>.
        /// </summary>
        /// <remarks>
        /// Multiple listeners can be registered. Exceptions thrown by a listener are swallowed to
        /// ensure they cannot disrupt the stream.
        /// </remarks>
        /// <param name="listener">The listener to register.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder AddStateChangeListener(IStreamStateChangeListener listener)
        {
            _streamNotificationReceiver.AddStreamStateChangeListener(listener);
            return this;
        }

        /// <summary>
        /// Subscribes an <see cref="IFailureListener"/> to receive a notification each time
        /// the stream encounters an unhandled exception and transitions to a failure state.
        /// </summary>
        /// <remarks>
        /// Multiple listeners can be registered. Exceptions thrown by a listener are swallowed to
        /// ensure they cannot disrupt the stream.
        /// </remarks>
        /// <param name="listener">The listener to register.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder AddFailureListener(IFailureListener listener)
        {
            _streamNotificationReceiver.AddFailureListener(listener);
            return this;
        }

        /// <summary>
        /// Subscribes an <see cref="ICheckFailureListener"/> to receive a notification each time
        /// a data quality check within the stream fails.
        /// </summary>
        /// <remarks>
        /// Multiple listeners can be registered. Exceptions thrown by a listener are swallowed to
        /// ensure they cannot disrupt the stream.
        /// </remarks>
        /// <param name="listener">The listener to register.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder AddCheckFailureListener(ICheckFailureListener listener)
        {
            _streamNotificationReceiver.AddCheckFailureListener(listener);
            return this;
        }

        /// <summary>
        /// Wires up an <see cref="IOptionsMonitor{FlowtidePauseOptions}"/> so that the stream can be
        /// dynamically paused and resumed at runtime through configuration changes without restarting.
        /// </summary>
        /// <remarks>
        /// If <see cref="FlowtidePauseOptions.IsPaused"/> is already <see langword="true"/> when
        /// <see cref="Build"/> is called, the stream starts in a paused state. Subsequent changes
        /// to the options monitor are applied immediately via the monitor's change callback.
        /// </remarks>
        /// <param name="pauseMonitor">The options monitor to observe for pause/resume state changes.</param>
        /// <returns>This builder instance for method chaining.</returns>
        public DataflowStreamBuilder WithPauseMonitor(IOptionsMonitor<FlowtidePauseOptions> pauseMonitor)
        {
            _pauseMonitor = pauseMonitor;
            return this;
        }

        /// <summary>
        /// Sets the semantic version string for this stream configuration.
        /// </summary>
        /// <remarks>
        /// Unlike <see cref="SetVersionInformation"/>, this method sets only the human-readable version label
        /// without a structural hash. It is stored alongside persisted state for diagnostic and upgrade tracking purposes.
        /// </remarks>
        /// <param name="version">
        /// A non-null, non-whitespace version string identifying this stream configuration.
        /// </param>
        /// <returns>This builder instance for method chaining.</returns>
        /// <exception cref="ArgumentException">Thrown if <paramref name="version"/> is null or whitespace.</exception>
        public DataflowStreamBuilder SetVersion(string version)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(version);
            _version = version;
            return this;
        }

        /// <summary>
        /// Validates the configuration, applies defaults for any unset optional components, and constructs
        /// the fully configured <see cref="DataflowStream"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If <see cref="WithStateHandler"/> was not called, a no-op <c>NullStateHandler</c> is used.
        /// If <see cref="WithStreamScheduler"/> was not called, a <see cref="DefaultStreamScheduler"/> is created.
        /// </para>
        /// <para>
        /// The returned <see cref="DataflowStream"/> is not yet started. Call
        /// <see cref="DataflowStream.StartAsync"/> or <see cref="DataflowStream.RunAsync"/> to begin processing.
        /// </para>
        /// </remarks>
        /// <returns>A fully configured but not yet started <see cref="DataflowStream"/>.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if <see cref="WithStateOptions"/> has not been called before <see cref="Build"/>.
        /// </exception>
        public DataflowStream Build()
        {
            if (_stateManagerOptions == null)
            {
                throw new InvalidOperationException("State options must be set.");
            }
            if (_stateHandler == null)
            {
                _stateHandler = new NullStateHandler();
            }
            if (_streamScheduler == null)
            {
                _streamScheduler = new DefaultStreamScheduler();
            }

            var streamContext = new StreamContext(
                _streamName,
                _version,
                _propagatorBlocks,
                _ingressBlocks,
                _egressBlocks,
                _stateHandler,
                _state,
                _streamScheduler,
                _streamNotificationReceiver,
                _stateManagerOptions,
                _loggerFactory,
                _streamVersionInformation,
                _dataflowStreamOptions,
                new StreamMemoryManager(_streamName),
                _pauseMonitor);

            return new DataflowStream(streamContext);
        }
    }
}
