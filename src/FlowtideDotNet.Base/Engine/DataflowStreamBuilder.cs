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

        public DataflowStreamBuilder(string streamName)
        {
            _streamName = streamName;
            _dataflowStreamOptions = new DataflowStreamOptions();
            _streamNotificationReceiver = new StreamNotificationReceiver(streamName);
        }

        public DataflowStreamBuilder AddPropagatorBlock(string name, IStreamVertex block)
        {
            block.Setup(_streamName, name);
            _propagatorBlocks.Add(name, block);
            return this;
        }

        public DataflowStreamBuilder AddIngressBlock(string name, IStreamIngressVertex block)
        {
            block.Setup(_streamName, name);
            _ingressBlocks.Add(name, block);
            return this;
        }

        public DataflowStreamBuilder AddEgressBlock(string name, IStreamEgressVertex block)
        {
            block.Setup(_streamName, name);
            _egressBlocks.Add(name, block);
            return this;
        }

        public DataflowStreamBuilder FromState(StreamState state)
        {
            _state = state;
            return this;
        }

        public DataflowStreamBuilder WithStreamScheduler(IStreamScheduler streamScheduler)
        {
            _streamScheduler = streamScheduler;
            return this;
        }

        public DataflowStreamBuilder WithStateHandler(IStateHandler stateHandler)
        {
            _stateHandler = stateHandler;
            return this;
        }

        public DataflowStreamBuilder WithStateOptions(StateManagerOptions stateManagerOptions)
        {
            _stateManagerOptions = stateManagerOptions;
            return this;
        }

        public DataflowStreamBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            return this;
        }

        public DataflowStreamBuilder SetVersionInformation(string hash, string version)
        {
            _streamVersionInformation = new StreamVersionInformation(hash, version);
            return this;
        }

        public DataflowStreamBuilder WaitForCheckpointAfterInitialData(bool wait)
        {
            _dataflowStreamOptions.WaitForCheckpointAfterInitialData = wait;
            return this;
        }

        public DataflowStreamBuilder SetMinimumTimeBetweenCheckpoint(TimeSpan timeSpan)
        {
            _dataflowStreamOptions.MinimumTimeBetweenCheckpoints = timeSpan;
            return this;
        }

        public DataflowStreamBuilder AddCheckpointListener(ICheckpointListener listener)
        {
            _streamNotificationReceiver.AddCheckpointListener(listener);
            return this;
        }

        public DataflowStreamBuilder AddStateChangeListener(IStreamStateChangeListener listener)
        {
            _streamNotificationReceiver.AddStreamStateChangeListener(listener);
            return this;
        }

        public DataflowStreamBuilder AddFailureListener(IFailureListener listener)
        {
            _streamNotificationReceiver.AddFailureListener(listener);
            return this;
        }

        public DataflowStreamBuilder WithPauseMonitor(IOptionsMonitor<FlowtidePauseOptions> pauseMonitor)
        {
            _pauseMonitor = pauseMonitor;
            return this;
        }

        public DataflowStreamBuilder SetVersion(string version)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(version);
            _version = version;
            return this;
        }

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
