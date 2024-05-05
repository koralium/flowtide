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

using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core
{
    public class ConnectorManager : IConnectorManager
    {
        private List<IConnectorSinkFactory> _connectorSinkFactories = new List<IConnectorSinkFactory>();
        private List<IConnectorSourceFactory> _connectorSourceFactories = new List<IConnectorSourceFactory>();
        private List<IConnectorTableProviderFactory> _connectorTableProviderFactories = new List<IConnectorTableProviderFactory>();

        public void AddSink(IConnectorSinkFactory connectorSinkFactory)
        {
            _connectorSinkFactories.Add(connectorSinkFactory);
        }

        public void AddSource(IConnectorSourceFactory connectorSourceFactory)
        {
            if (connectorSourceFactory is IConnectorTableProviderFactory tableProviderFactory)
            {
                _connectorTableProviderFactories.Add(tableProviderFactory);
            }
            _connectorSourceFactories.Add(connectorSourceFactory);
        }

        public IConnectorSinkFactory? GetSinkFactory(WriteRelation writeRelation)
        {
            foreach(var factory in _connectorSinkFactories)
            {
                if (factory.CanHandle(writeRelation))
                {
                    return factory;
                }
            }
            return default;
        }

        public IConnectorSourceFactory? GetSourceFactory(ReadRelation readRelation)
        {
            foreach (var factory in _connectorSourceFactories)
            {
                if (factory.CanHandle(readRelation))
                {
                    return factory;
                }
            }
            return default;
        }

        public IEnumerable<ITableProvider> GetTableProviders()
        {
            return _connectorTableProviderFactories.Select(f => f.Create());
        }
    }
}
