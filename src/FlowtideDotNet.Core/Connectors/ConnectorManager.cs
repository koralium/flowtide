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
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core
{
    public class ConnectorManager : IConnectorManager
    {
        private readonly List<IConnectorSinkFactory> _connectorSinkFactories = new List<IConnectorSinkFactory>();
        private readonly List<IConnectorSourceFactory> _connectorSourceFactories = new List<IConnectorSourceFactory>();
        private readonly List<IConnectorTableProviderFactory> _connectorTableProviderFactories = new List<IConnectorTableProviderFactory>();
        private readonly List<ITableProvider> _tableProviders = new List<ITableProvider>();
        private SortedList<string, ICatalogConnectorManager> _catalogs = new SortedList<string, ICatalogConnectorManager>();

        public void AddCatalog(string catalogName, Action<ICatalogConnectorManager> options)
        {
            var catalogManager = new CatalogConnectorManager(catalogName);
            options(catalogManager);
            _catalogs.Add(catalogName,catalogManager);
            _tableProviders.Add(catalogManager);
        }

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

        public void AddTableProvider(ITableProvider tableProvider)
        {
            _tableProviders.Add(tableProvider);
        }

        public IConnectorSinkFactory GetSinkFactory(WriteRelation writeRelation)
        {
            if (_catalogs.TryGetValue(writeRelation.NamedObject.Names[0], out var catalog))
            {
                return catalog.GetSinkFactory(writeRelation);
            }
            var possibleConnectors = _connectorSinkFactories.Where(x => x.CanHandle(writeRelation));
            var count = possibleConnectors.Count();
            if (count > 1)
            {
                throw new FlowtideDuplicateConnectorsException($"Multiple connectors can handle the write relation '{writeRelation.NamedObject.DotSeperated}'.");
            }
            if (count == 0)
            {
                throw new FlowtideNoConnectorFoundException($"No connector can handle the write relation '{writeRelation.NamedObject.DotSeperated}'.");
            }
            return possibleConnectors.First();
        }

        public IConnectorSourceFactory GetSourceFactory(ReadRelation readRelation)
        {
            if (_catalogs.TryGetValue(readRelation.NamedTable.Names[0], out var catalog))
            {
                return catalog.GetSourceFactory(readRelation);
            }
            var possibleConnectors = _connectorSourceFactories.Where(x => x.CanHandle(readRelation));
            var count = possibleConnectors.Count();
            if (count > 1)
            {
                throw new FlowtideDuplicateConnectorsException($"Multiple connectors can handle the read relation '{readRelation.NamedTable.DotSeperated}'.");
            }
            if (count == 0)
            {
                throw new FlowtideNoConnectorFoundException($"No connector can handle the read relation '{readRelation.NamedTable.DotSeperated}'.");
            }
            return possibleConnectors.First();
        }

        public IEnumerable<ITableProvider> GetTableProviders()
        {
            return _connectorTableProviderFactories.Select(f => f.Create())
                .Union(_tableProviders);
        }
    }
}
