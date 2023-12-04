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
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;

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
        private IStreamScheduler? _streamScheduler;
        private IStreamNotificationReciever? _streamNotificationReciever;
        private StateManagerOptions? _stateManagerOptions;
        private ILoggerFactory? _loggerFactory;
        private StreamVersionInformation? _streamVersionInformation;

        public DataflowStreamBuilder(string streamName)
        {
            _streamName = streamName;
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

        public DataflowStreamBuilder WithNotificationReciever(IStreamNotificationReciever notificationReciever)
        {
            _streamNotificationReciever = notificationReciever;
            return this;
        }

        public DataflowStreamBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            return this;
        }

        public DataflowStreamBuilder SetVersionInformation(long streamVersion, string hash)
        {
            _streamVersionInformation = new StreamVersionInformation(streamVersion, hash);
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
                _propagatorBlocks, 
                _ingressBlocks, 
                _egressBlocks, 
                _stateHandler, 
                _state, 
                _streamScheduler,
                _streamNotificationReciever,
                _stateManagerOptions,
                _loggerFactory,
                _streamVersionInformation);

            return new DataflowStream(streamContext);
        }
    }
}
