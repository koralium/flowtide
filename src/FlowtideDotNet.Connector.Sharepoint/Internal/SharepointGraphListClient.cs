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
using FlowtideDotNet.Connector.Sharepoint.Internal.Encoders;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Kiota.Abstractions;
using System.Net.Http.Json;
using System.Text;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal class SharepointGraphListClient
    {
        private readonly string graphSite;
        private readonly string sharepointUrl;
        private readonly string site;
        private readonly TokenCredential tokenCredential;
        private readonly GraphServiceClient graphClient;
        private string? siteId;
        private readonly HttpClient httpClient = new HttpClient();
        private AccessToken? ensureUserToken;
        private readonly Dictionary<string, int> _userIds = new Dictionary<string, int>();
        private readonly SharepointSinkOptions sharepointSinkOptions;
        private readonly ILogger logger;

        public SharepointGraphListClient(SharepointSinkOptions sharepointSinkOptions, ILogger logger)
        {
            this.graphSite = $"{sharepointSinkOptions.SharepointUrl}:/sites/{sharepointSinkOptions.Site}:";
            this.sharepointUrl = sharepointSinkOptions.SharepointUrl;
            this.site = sharepointSinkOptions.Site;
            this.tokenCredential = sharepointSinkOptions.TokenCredential;
            graphClient = new GraphServiceClient(tokenCredential);
            this.sharepointSinkOptions = sharepointSinkOptions;
            this.logger = logger;
        }

        public async Task Initialize()
        {
            var siteObj = await graphClient.Sites[graphSite].GetAsync();

            if (siteObj == null)
            {
                throw new InvalidOperationException("Site does not exist");
            }

            siteId = siteObj.Id;
        }

        public ValueTask<int?> EnsureUser(string upn)
        {
            if (_userIds.TryGetValue(upn, out var user))
            {
                return ValueTask.FromResult<int?>(user);
            }
            return EnsureUser_Slow(upn);
        }

        private async ValueTask<int?> EnsureUser_Slow(string upn)
        {
            var req = new HttpRequestMessage(HttpMethod.Post, $"https://{sharepointUrl}/sites/{site}/_api/web/ensureUser")
            {
                Content = new StringContent($"{{\"logonName\":\"{upn}\"}}", Encoding.UTF8, "application/json")
            };
            if (ensureUserToken == null || ensureUserToken.Value.ExpiresOn.CompareTo(DateTimeOffset.UtcNow.Add(TimeSpan.FromMinutes(10))) < 0)
            {
                ensureUserToken = await tokenCredential.GetTokenAsync(new TokenRequestContext(new string[] { $"https://{sharepointUrl}/.default" }), default);
            }
            req.Headers.Add("Authorization", $"Bearer {ensureUserToken.Value.Token}");
            req.Headers.Add("Accept", "application/json");
            var response = await httpClient.SendAsync(req);
            if (response.StatusCode != System.Net.HttpStatusCode.OK)
            {
                var err = await response.Content.ReadAsStringAsync();
                if (sharepointSinkOptions.ThrowOnPersonOrGroupNotFound)
                {
                    throw new InvalidOperationException($"Person or group not found: {err}");
                }
                logger.LogWarning($"Person or group not found: {err}");
                return default;
            }
            var ensureUserResult = await response.Content.ReadFromJsonAsync<EnsureUserResult>();

            if (ensureUserResult == null || ensureUserResult.Id == null)
            {
                if (sharepointSinkOptions.ThrowOnPersonOrGroupNotFound)
                {
                    throw new InvalidOperationException($"Person or group not found: unknown error.");
                }
                logger.LogWarning($"Person or group not found: unknown error.");
                return default;
            }
            _userIds[upn] = ensureUserResult.Id.Value;
            return ensureUserResult.Id.Value;
        }

        public async Task<List<IColumnEncoder>> GetColumnEncoders(string list, List<string> columns, IStateManagerClient stateManagerClient)
        {
            var listColumns = await graphClient.Sites[graphSite].Lists[list].Columns.GetAsync();

            if (listColumns?.Value == null)
            {
                throw new InvalidOperationException($"Could not find list {list}");
            }

            List<IColumnEncoder> output = new List<IColumnEncoder>();
            foreach(var column in columns)
            {
                var col = listColumns.Value.Find(x => x.Name?.Equals(column, StringComparison.OrdinalIgnoreCase) ?? false);
                if (col == null)
                {
                    throw new InvalidOperationException($"Could not find column {column}");
                }
                var encoder = GetColumnEncoder(col);
                await encoder.Initialize(column, this, stateManagerClient.GetChildManager(column), col);
                output.Add(encoder);
            }
            return output;
        }

        public async Task<string> GetListId(string listName)
        {
            var list = await graphClient.Sites[graphSite].Lists[listName].GetAsync();
            if (list == null)
            {
                throw new InvalidOperationException($"Could not find list {listName}");
            }
            return list.Id!;
        }

        public BatchRequestContentCollection NewBatch()
        {
            return new BatchRequestContentCollection(graphClient);
        }

        public Task<BatchResponseContentCollection> ExecuteBatch(BatchRequestContentCollection batch)
        {
            return graphClient.Batch.PostAsync(batch);
        }

        private static IColumnEncoder GetColumnEncoder(ColumnDefinition columnDefinition)
        {
            if (columnDefinition.Text != null)
            {
                return new TextEncoder();
            }
            if (columnDefinition.PersonOrGroup != null)
            {
                return new GroupPersonEncoder();
            }
            if (columnDefinition.Boolean != null)
            {
                return new BooleanEncoder();
            }
            if (columnDefinition.DateTime != null)
            {
                return new DateTimeEncoder();
            }
            if (columnDefinition.Choice != null)
            {
                return new ChoiceEncoder();
            }
            if (columnDefinition.Number != null)
            {
                return new NumberEncoder();
            }
            if (columnDefinition.Currency != null)
            {
                return new CurrencyEncoder();
            }
            throw new NotImplementedException();
        }

        public async Task<string> CreateItemAsync(string list, Dictionary<string, object> obj)
        {
            var item = await graphClient.Sites[siteId].Lists[list].Items.PostAsync(new ListItem()
            {
                Fields = new FieldValueSet()
                {
                    AdditionalData = obj
                }
            });
            if (item == null) 
            { 
                throw new InvalidOperationException("Could not create item"); 
            }
            return item.Id!;
        }

        public RequestInformation CreateItemBatch(string list, Dictionary<string, object> obj)
        {
            return graphClient.Sites[siteId].Lists[list].Items.ToPostRequestInformation(new ListItem()
            {
                Fields = new FieldValueSet()
                {
                    AdditionalData = obj
                }
            });
        }

        public HttpRequestMessage CreateItemBatchHttpRequest(string list, Dictionary<string, object> obj)
        {
            var req = graphClient.Sites[siteId].Lists[list].Items.ToPostRequestInformation(new ListItem()
            {
                Fields = new FieldValueSet()
                {
                    AdditionalData = obj
                }
            });

            using var reader = new StreamReader(req.Content);
            var content = reader.ReadToEnd();
            return new HttpRequestMessage(HttpMethod.Post, req.URI)
            {
                Content = new StringContent(content, Encoding.UTF8, "application/json")
            };
        }

        public RequestInformation UpdateItemBatch(string list, string id, Dictionary<string, object> obj)
        {
            return graphClient.Sites[siteId].Lists[list].Items[id].Fields.ToPatchRequestInformation(new FieldValueSet()
            {
                AdditionalData = obj
            });
        }

        public HttpRequestMessage UpdateItemBatchHttpRequest(string list, string id, Dictionary<string, object> obj)
        {
            var req = graphClient.Sites[siteId].Lists[list].Items[id].Fields.ToPatchRequestInformation(new FieldValueSet()
            {
                AdditionalData = obj
            });

            using var reader = new StreamReader(req.Content);
            var content = reader.ReadToEnd();
            return new HttpRequestMessage(HttpMethod.Patch, req.URI)
            {
                Content = new StringContent(content, Encoding.UTF8, "application/json")
            };
        }

        public async Task UpdateItemAsync(string list, string id, Dictionary<string, object> obj)
        {
            await graphClient.Sites[siteId].Lists[list].Items[id].Fields.PatchAsync(new FieldValueSet()
            {
                AdditionalData = obj
            });
        }

        public HttpRequestMessage DeleteItemBatchHttpRequest(string list, string id)
        {
            var req = graphClient.Sites[siteId].Lists[list].Items[id].ToDeleteRequestInformation();

            using var reader = new StreamReader(req.Content);
            var content = reader.ReadToEnd();
            return new HttpRequestMessage(HttpMethod.Delete, req.URI)
            {
                Content = new StringContent(content, Encoding.UTF8, "application/json")
            };
        }

        public async Task IterateList(string list, List<string> columns, Func<ListItem, Task<bool>> onItem)
        {
            var getListReq = await graphClient.Sites[siteId].Lists[list].Items.GetAsync(b =>
            {
                b.QueryParameters.Expand = new string[]
                {
                    $"fields($select=Id,{string.Join(",", columns)})"
                };
            });

            if (getListReq == null)
            {
                throw new InvalidOperationException("Could not get list");
            }
            var iterator = PageIterator<ListItem, ListItemCollectionResponse>.CreatePageIterator(graphClient, getListReq, onItem);
            await iterator.IterateAsync();
        }
    }
}
