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

using FlowtideDotNet.Core.Operators.Exchange;

namespace FlowtideDotNet.Core.Engine.Distributed
{
    /// <summary>
    /// Connects substreams that run in the same process with each other.
    /// Each substream gets its own <see cref="ISubstreamCommunicationHandlerFactory"/> from
    /// <see cref="CreateFactory"/>, messages sent from one substream are dispatched directly
    /// to the callbacks registered by the other substream.
    /// </summary>
    public class LocalSubstreamCommunicationHub
    {
        private readonly object _lock = new object();
        private readonly Dictionary<(string Self, string Target), LocalSubstreamCommunicationHandler> _handlers = new Dictionary<(string, string), LocalSubstreamCommunicationHandler>();

        /// <summary>
        /// Creates the communication handler factory for one substream,
        /// used with <see cref="DistributedOptions"/>.
        /// </summary>
        public ISubstreamCommunicationHandlerFactory CreateFactory(string substreamName)
        {
            return new LocalSubstreamCommunicationFactory(this, substreamName);
        }

        internal LocalSubstreamCommunicationHandler GetOrCreateHandler(string selfSubstreamName, string targetSubstreamName)
        {
            lock (_lock)
            {
                if (!_handlers.TryGetValue((selfSubstreamName, targetSubstreamName), out var handler))
                {
                    handler = new LocalSubstreamCommunicationHandler(this, selfSubstreamName, targetSubstreamName);
                    _handlers.Add((selfSubstreamName, targetSubstreamName), handler);
                }
                return handler;
            }
        }

        /// <summary>
        /// Tries to get the handler that the target substream registered towards the calling substream.
        /// Returns false if the target substream has not initialized its side of the connection yet.
        /// </summary>
        internal bool TryGetPeerHandler(string selfSubstreamName, string targetSubstreamName, out LocalSubstreamCommunicationHandler handler)
        {
            lock (_lock)
            {
                if (_handlers.TryGetValue((targetSubstreamName, selfSubstreamName), out var found) && found.IsInitialized)
                {
                    handler = found;
                    return true;
                }
            }
            handler = default!;
            return false;
        }

        private sealed class LocalSubstreamCommunicationFactory : ISubstreamCommunicationHandlerFactory
        {
            private readonly LocalSubstreamCommunicationHub _hub;
            private readonly string _substreamName;

            public LocalSubstreamCommunicationFactory(LocalSubstreamCommunicationHub hub, string substreamName)
            {
                _hub = hub;
                _substreamName = substreamName;
            }

            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
            {
                return _hub.GetOrCreateHandler(selfSubstreamName, targetSubstreamName);
            }
        }
    }
}
