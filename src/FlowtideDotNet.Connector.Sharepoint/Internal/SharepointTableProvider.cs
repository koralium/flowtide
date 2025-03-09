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

using Azure.Core;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal class SharepointTableProvider : ITableProvider
    {
        private readonly SharepointSourceOptions _sharepointSourceOptions;
        private readonly Dictionary<string, TableMetadata> _tables = new Dictionary<string, TableMetadata>();
        private readonly GraphServiceClient _graphClient;
        private ListCollectionResponse? _listResponse;
        private readonly string _prefix;

        public SharepointTableProvider(SharepointSourceOptions sharepointSourceOptions, string? prefix = null)
        {
            _sharepointSourceOptions = sharepointSourceOptions;
            _graphClient = new GraphServiceClient(_sharepointSourceOptions.TokenCredential);
            _prefix = prefix ?? "";
        }
        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            string fullName = string.Join(".", tableName);
            TryLoadSharepointData();
            if (_listResponse == null || _listResponse.Value == null)
            {
                throw new InvalidOperationException("Could not fetch sharepoint information");
            }

            if (!fullName.StartsWith(_prefix))
            {
                tableMetadata = null;
                return false;
            }
            fullName = fullName.Substring(_prefix.Length);

            var list = _listResponse.Value.Find(x => x.Name == fullName);
            if (list == null)
            {
                tableMetadata = null;
                return false;
            }
            tableMetadata = GetListData(list);
            return true;
        }

        internal string GetListId(string tableName)
        {
            TryLoadSharepointData();
            if (_listResponse == null || _listResponse.Value == null)
            {
                throw new InvalidOperationException("Could not fetch sharepoint information");
            }

            if (!tableName.StartsWith(_prefix))
            {
                throw new InvalidOperationException("Table name does not start with prefix");
            }
            tableName = tableName.Substring(_prefix.Length);

            var list = _listResponse.Value.Find(x => x.Name == tableName);
            if (list == null || list.Id == null)
            {
                throw new InvalidOperationException("Could not find list");
            }
            return list.Id;
        }

        private TableMetadata GetListData(List list)
        {
            if (list.Id == null)
            {
                throw new InvalidOperationException("List id is null");
            }
            if (_tables.TryGetValue(list.Id, out var tableMetadata))
            {
                return tableMetadata;
            }
            return LoadSharepointListData(list).GetAwaiter().GetResult();
        }

        private void TryLoadSharepointData()
        {
            if (_listResponse == null)
            {
                LoadSharepointData().Wait();
            }
        }

        private async Task<TableMetadata> LoadSharepointListData(List list)
        {
            if (list.Id == null)
            {
                throw new InvalidOperationException("List id is null");
            }

            var graphSite = $"{_sharepointSourceOptions.SharepointUrl}:/sites/{_sharepointSourceOptions.Site}:";
            var columns = await _graphClient.Sites[graphSite].Lists[list.Id].Columns.GetAsync();

            if (columns == null)
            {
                throw new InvalidOperationException("Could not fetch sharepoint list columns");
            }
            if (columns.Value == null)
            {
                throw new InvalidOperationException("Could not fetch sharepoint list columns");
            }

            List<string> columnNames = new List<string>();
            List<SubstraitBaseType> types = new List<SubstraitBaseType>();

            foreach (var column in columns.Value)
            {
                if (column.Name == null)
                {
                    continue;
                }
                columnNames.Add(column.Name);
                types.Add(new AnyType());
            }
            columnNames.Add("_fields");
            types.Add(new AnyType());

            if (list.Name == null)
            {
                throw new InvalidOperationException("List name is null");
            }

            var metadata = new TableMetadata(list.Name, new NamedStruct()
            {
                Names = columnNames,
                Struct = new Struct()
                {
                    Types = types
                }
            });
            _tables.Add(list.Id, metadata);
            return metadata;
        }

        private async Task LoadSharepointData()
        {
            var graphSite = $"{_sharepointSourceOptions.SharepointUrl}:/sites/{_sharepointSourceOptions.Site}:";
            _listResponse = await _graphClient.Sites[graphSite].Lists.GetAsync();
        }
    }
}
