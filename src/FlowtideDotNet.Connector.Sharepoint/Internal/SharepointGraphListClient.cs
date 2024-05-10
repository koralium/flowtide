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
using FlexBuffers;
using FlowtideDotNet.Connector.Sharepoint.Internal.Decoders;
using FlowtideDotNet.Connector.Sharepoint.Internal.Encoders;
using FlowtideDotNet.Core;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Microsoft.Kiota.Abstractions;
using Microsoft.Kiota.Http.HttpClientLibrary.Middleware;
using Microsoft.Kiota.Http.HttpClientLibrary.Middleware.Options;
using System.Buffers;
using System.Diagnostics;
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
        private readonly HttpClient httpClient;
        private AccessToken? ensureUserToken;
        private readonly Dictionary<string, int> _userIds = new Dictionary<string, int>();
        private readonly SharepointOptions sharepointSinkOptions;
        private readonly string streamName;
        private readonly string operatorId;
        private readonly ILogger logger;

        public SharepointGraphListClient(SharepointOptions sharepointSinkOptions, string streamName, string operatorId, ILogger logger)
        {
            this.graphSite = $"{sharepointSinkOptions.SharepointUrl}:/sites/{sharepointSinkOptions.Site}:";
            this.sharepointUrl = sharepointSinkOptions.SharepointUrl;
            this.site = sharepointSinkOptions.Site;
            this.tokenCredential = new AccessTokenCacheProvider(sharepointSinkOptions.TokenCredential);

            httpClient = new HttpClient();
            graphClient = new GraphServiceClient(tokenCredential);
            this.sharepointSinkOptions = sharepointSinkOptions;
            this.streamName = streamName;
            this.operatorId = operatorId;
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
            int retryCount = 0;
            HttpResponseMessage? response = default;
            while (true)
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
                response = await httpClient.SendAsync(req);

                if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests || response.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable)
                {
                    TimeSpan delay = TimeSpan.FromSeconds(Math.Max(300, Math.Pow(2, retryCount) * 3)); ;
                    if (response.Headers.RetryAfter != null)
                    {
                        if (response.Headers.RetryAfter.Delta.HasValue)
                        {
                            delay = response.Headers.RetryAfter.Delta.Value;
                        }
                        else if (response.Headers.RetryAfter.Date.HasValue)
                        {
                            delay = response.Headers.RetryAfter.Date.Value - DateTime.UtcNow;
                        }
                    }
                    retryCount++;

                    logger.LogWarning("Rate limited, retrying in {delay} seconds", delay.TotalSeconds);

                    await Task.Delay(delay);
                    continue;
                }
                else
                {
                    break;
                }
            }
            
            if (response.StatusCode != System.Net.HttpStatusCode.OK)
            {
                var err = await response.Content.ReadAsStringAsync();
                if (sharepointSinkOptions.ThrowOnPersonOrGroupNotFound)
                {
                    throw new InvalidOperationException($"Person or group not found: {err}");
                }
                logger.PersonOrGroupNotFound(err, streamName, operatorId);
                _userIds[upn] = default;
                return default;
            }
            var ensureUserResult = await response.Content.ReadFromJsonAsync<EnsureUserResult>();

            if (ensureUserResult == null || ensureUserResult.Id == null)
            {
                if (sharepointSinkOptions.ThrowOnPersonOrGroupNotFound)
                {
                    throw new InvalidOperationException($"Person or group not found: unknown error.");
                }
                logger.PersonOrGroupNotFound("Unknown error", streamName, operatorId);
                return default;
            }
            _userIds[upn] = ensureUserResult.Id.Value;
            return ensureUserResult.Id.Value;
        }

        public async Task<IDictionary<string, ColumnDefinition>> GetColumns(string listId)
        {
            var listColumns = await graphClient.Sites[graphSite].Lists[listId].Columns.GetAsync();

            if (listColumns == null || listColumns.Value == null)
            {
                throw new InvalidOperationException($"Could not find list {listId}");
            }

            var columnsDict = listColumns.Value.ToDictionary(x => x.Name ?? throw new InvalidOperationException("Recieved column without name"));

            return columnsDict;
        }

        public async Task<Dictionary<string, IColumnDecoder>> GetColumnDecoders(string list, List<string> columnNames, IStateManagerClient stateManagerClient)
        {
            var columnsDict = await GetColumns(list);

            Dictionary<string, IColumnDecoder> output = new Dictionary<string, IColumnDecoder>();
            foreach(var column in columnNames)
            {
                var encoder = GetColumnDecoder(column, columnsDict);
                await encoder.Initialize(column, list, this, stateManagerClient.GetChildManager(column), columnsDict);
                output.Add(column, encoder);
            }
            return output;
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

        private static IColumnDecoder GetColumnDecoder(string columnName, IDictionary<string, ColumnDefinition> columns)
        {
            if (columns.TryGetValue(columnName, out var columnDefinition))
            {
                if (columnDefinition.Text != null)
                {
                    return new TextDecoder();
                }
                if (columnDefinition.Lookup != null)
                {
                    return new LookupDecoder();
                }
                if (columnDefinition.Boolean != null)
                {
                    return new BooleanDecoder();
                }
                if (columnDefinition.DateTime != null)
                {
                    return new DateTimeDecoder();
                }
                if (columnDefinition.PersonOrGroup != null)
                {
                    return new GroupPersonDecoder();
                }
            }

            return GetSpecialColumnDecoder(columnName);
        }

        private static IColumnDecoder GetSpecialColumnDecoder(string columnName)
        {
            // Special columns
            if (columnName.Equals("ID", StringComparison.OrdinalIgnoreCase))
            {
                return new IdDecoder();
            }
            if (columnName.Equals("ContentType", StringComparison.OrdinalIgnoreCase))
            {
                return new ContentTypeDecoder();
            }
            if (columnName.Equals("Attachments", StringComparison.OrdinalIgnoreCase))
            {
                return new AttachmentsDecoder();
            }
            if (columnName.Equals("Edit", StringComparison.OrdinalIgnoreCase))
            {
                return new EditDecoder();
            }
            if (columnName.Equals("LinkTitleNoMenu", StringComparison.OrdinalIgnoreCase))
            {
                return new LinkTitleNoMenuDecoder();
            }
            if (columnName.Equals("LinkTitle", StringComparison.OrdinalIgnoreCase))
            {
                return new LinkTitleDecoder();
            }
            if (columnName.Equals("DocIcon", StringComparison.OrdinalIgnoreCase))
            {
                return new DocIconDecoder();
            }
            if (columnName.Equals("_IsRecord", StringComparison.OrdinalIgnoreCase))
            {
                return new IsRecordDecoder();
            }
            if (columnName.Equals("_fields", StringComparison.OrdinalIgnoreCase))
            {
                return new FieldsDecoder();
            }
            throw new NotImplementedException();
        }

        private static IColumnEncoder GetColumnEncoder(ColumnDefinition columnDefinition)
        {
            if (columnDefinition.Text != null)
            {
                return new TextEncoder();
            }
            if (columnDefinition.PersonOrGroup != null)
            {
                return new GroupPersonEncoder(columnDefinition.PersonOrGroup.AllowMultipleSelection ?? false);
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

        public (HttpRequestMessage, string) CreateItemBatchHttpRequest(string list, Dictionary<string, object> obj)
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
            return (new HttpRequestMessage(HttpMethod.Post, req.URI)
            {
                Content = new StringContent(content, Encoding.UTF8, "application/json")
            }, content);
        }

        public RequestInformation UpdateItemBatch(string list, string id, Dictionary<string, object> obj)
        {
            return graphClient.Sites[siteId].Lists[list].Items[id].Fields.ToPatchRequestInformation(new FieldValueSet()
            {
                AdditionalData = obj
            });
        }

        public (HttpRequestMessage, string) UpdateItemBatchHttpRequest(string list, string id, Dictionary<string, object> obj)
        {
            var req = graphClient.Sites[siteId].Lists[list].Items[id].Fields.ToPatchRequestInformation(new FieldValueSet()
            {
                AdditionalData = obj
            });

            using var reader = new StreamReader(req.Content);
            var content = reader.ReadToEnd();
            return (new HttpRequestMessage(HttpMethod.Patch, req.URI)
            {
                Content = new StringContent(content, Encoding.UTF8, "application/json")
            }, content);
        }

        public async Task UpdateItemAsync(string list, string id, Dictionary<string, object> obj)
        {
            await graphClient.Sites[siteId].Lists[list].Items[id].Fields.PatchAsync(new FieldValueSet()
            {
                AdditionalData = obj
            });
        }

        public (HttpRequestMessage, string) DeleteItemBatchHttpRequest(string list, string id)
        {
            var req = graphClient.Sites[siteId].Lists[list].Items[id].ToDeleteRequestInformation();

            using var reader = new StreamReader(req.Content);
            var content = reader.ReadToEnd();
            return (new HttpRequestMessage(HttpMethod.Delete, req.URI), content);
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

        public async IAsyncEnumerable<Microsoft.Graph.Sites.Item.Lists.Item.Items.Delta.DeltaGetResponse> GetDeltaFromList(string list)
        {
            var resp = await graphClient.Sites[siteId].Lists[list].Items.Delta.GetAsDeltaGetResponseAsync();
            if (resp == null)
            {
                yield break;
            }

            yield return resp;

            while (resp.OdataNextLink != null)
            {
                resp = await graphClient.Sites[siteId].Lists[list].Items.Delta.WithUrl(resp.OdataNextLink).GetAsDeltaGetResponseAsync();

                if (resp == null)
                {
                    yield break;
                }
                yield return resp;
            }
        }

        public async IAsyncEnumerable<Microsoft.Graph.Sites.Item.Lists.Item.Items.Delta.DeltaGetResponse> GetDeltaFromUrl(string list, string url)
        {
            var resp = await graphClient.Sites[siteId].Lists[list].Items.Delta.WithUrl(url).GetAsDeltaGetResponseAsync();
            if (resp == null)
            {
                yield break;
            }

            yield return resp;

            while (resp.OdataNextLink != null)
            {
                resp = await graphClient.Sites[siteId].Lists[list].Items.Delta.WithUrl(resp.OdataNextLink).GetAsDeltaGetResponseAsync();

                if (resp == null)
                {
                    yield break;
                }
                yield return resp;
            }
        }
    }
}
