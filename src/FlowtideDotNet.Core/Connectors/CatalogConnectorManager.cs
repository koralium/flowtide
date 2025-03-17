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

using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Connectors
{
    internal class CatalogConnectorManager : ICatalogConnectorManager, ITableProvider
    {
        private readonly string catalogName;
        private List<ITableProvider>? _resolvedTableProviders;

        private readonly List<IConnectorSinkFactory> _connectorSinkFactories = new List<IConnectorSinkFactory>();
        private readonly List<IConnectorSourceFactory> _connectorSourceFactories = new List<IConnectorSourceFactory>();
        private readonly List<IConnectorTableProviderFactory> _connectorTableProviderFactories = new List<IConnectorTableProviderFactory>();
        private readonly List<ITableProvider> _tableProviders = new List<ITableProvider>();

        public CatalogConnectorManager(string catalogName)
        {
            this.catalogName = catalogName;
        }

        public void AddCatalog(string catalogName, Action<ICatalogConnectorManager> options)
        {
            throw new NotSupportedException("Catalogs cannot be added to catalogs");
        }

        public void AddSink(IConnectorSinkFactory connectorSinkFactory)
        {
            _connectorSinkFactories.Add(new CatalogSinkFactory(catalogName, connectorSinkFactory));
        }

        public void AddSource(IConnectorSourceFactory connectorSourceFactory)
        {
            if (connectorSourceFactory is IConnectorTableProviderFactory tableProviderFactory)
            {
                _connectorTableProviderFactories.Add(tableProviderFactory);
            }
            _connectorSourceFactories.Add(new CatalogSourceFactory(catalogName, connectorSourceFactory));
        }

        public void AddTableProvider(ITableProvider tableProvider)
        {
            _tableProviders.Add(tableProvider);
        }

        public IConnectorSinkFactory GetSinkFactory(WriteRelation writeRelation)
        {
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

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (tableName.Count > 1 && tableName[0] == catalogName)
            {
                if (_resolvedTableProviders == null)
                {
                    _resolvedTableProviders = GetTableProviders().ToList();
                }
                for(int i = 0; i < _resolvedTableProviders.Count; i++)
                {
                    var nameWithoutCatalog = tableName.Skip(1).ToList();
                    if (_resolvedTableProviders[i].TryGetTableInformation(nameWithoutCatalog, out tableMetadata))
                    {
                        return true;
                    }
                }
            }
            tableMetadata = default;
            return false;
        }
    }
}
