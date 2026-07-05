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

using FlowtideDotNet.Core;

namespace FlowtideDotNet.Orleans.Internal
{
    /// <summary>
    /// Creates the connector manager for a stream. Each grain creates its own manager per
    /// stream instead of sharing one silo wide instance, so connectors can be configured
    /// per stream and connector factories holding per stream state are never shared.
    /// </summary>
    internal sealed class ConnectorManagerFactory
    {
        private readonly Action<string, IConnectorManager> _configure;

        public ConnectorManagerFactory(Action<string, IConnectorManager> configure)
        {
            _configure = configure;
        }

        public IConnectorManager Create(string streamName)
        {
            var connectorManager = new ConnectorManager();
            _configure(streamName, connectorManager);
            return connectorManager;
        }
    }
}
