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
        private ListCollectionResponse? _listResponse;

        public SharepointTableProvider(SharepointSourceOptions sharepointSourceOptions)
        {
            _sharepointSourceOptions = sharepointSourceOptions;
        }
        public bool TryGetTableInformation(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            TryLoadSharepointData();
            if (_listResponse == null || _listResponse.Value == null)
            {
                throw new InvalidOperationException("Could not fetch sharepoint information");
            }

            var list = _listResponse.Value.FirstOrDefault(x => x.Name == tableName);

            tableMetadata = null;
            return false;
        }

        private void TryLoadSharepointData()
        {
            if (_listResponse == null)
            {
                LoadSharepointData().Wait();
            }
        }

        private async Task LoadSharepointData()
        {
            var graphClient = new GraphServiceClient(_sharepointSourceOptions.TokenCredential);
            var graphSite = $"{_sharepointSourceOptions.SharepointUrl}:/sites/{_sharepointSourceOptions.Site}:";
            _listResponse = await graphClient.Sites[graphSite].Lists.GetAsync();
        }
    }
}
