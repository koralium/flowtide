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

using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;

namespace FlowtideDotNet.Connector.Starrocks.Internal.HttpApi
{
    internal class StarrocksHttpClient : IStarrocksClient
    {
        private readonly string _url;
        private readonly string _backendUrl;
        private readonly HttpClient _httpClient;
        private readonly AuthenticationHeaderValue _authenticationHeaderValue;
        private readonly StarrocksSinkOptions _options;
        private Dictionary<string, string> _redirectUris;

        public StarrocksHttpClient(StarrocksSinkOptions options)
        {
            _url = options.HttpUrl;
            _backendUrl = _url;

            if (options.BackendHttpUrl != null)
            {
                _backendUrl = options.BackendHttpUrl;
            }
            _redirectUris = new Dictionary<string, string>();
            _httpClient = new HttpClient(new StarrocksHttpClientHandler() { AllowAutoRedirect = false })
            {
                //BaseAddress = new Uri(url)
            };

            var authenticationString = $"{options.Username}:{options.Password}";
            var base64EncodedAuthenticationString = Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(authenticationString));

            _authenticationHeaderValue = new AuthenticationHeaderValue("Basic", base64EncodedAuthenticationString);
            _httpClient.DefaultRequestHeaders.Authorization = _authenticationHeaderValue;
            this._options = options;
        }

        public async Task StreamLoad(string database, string table, ReadOnlyMemory<byte> memory)
        {
            var jsonData = Encoding.UTF8.GetString(memory.Span);
            HttpResponseMessage? response = default;

            bool triedCachedRedirect = false;
            do
            {
                string? requestUrl = $"{_backendUrl}/api/{database}/{table}/_stream_load";
                bool usedRedirectUri = false;

                // First we try and use a cached redirect uri for the request to skip an extra roundtrip
                if (!triedCachedRedirect && _redirectUris.TryGetValue(requestUrl, out var redirectedUrl))
                {
                    requestUrl = redirectedUrl;
                    triedCachedRedirect = true;
                    usedRedirectUri = true;
                }
                // If we have a response it means we got redirected
                else if (response != null)
                {
                    if (response.Headers.Location == null)
                    {
                        throw new InvalidOperationException("Redirect response missing Location header.");
                    }
                    var newUrl = response.Headers.Location.ToString();
                    // Store the new redirect uri for future requests
                    _redirectUris[requestUrl] = newUrl;
                    requestUrl = newUrl;
                }

                var content = new ReadOnlyMemoryContent(memory);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                var requestMessage = new HttpRequestMessage(HttpMethod.Put, requestUrl)
                {
                    Content = content
                };
                requestMessage.Headers.Add("Expect", "100-continue");
                requestMessage.Headers.Authorization = _authenticationHeaderValue;
                requestMessage.Headers.Add("format", "json");
                requestMessage.Headers.Add("strip_outer_array", "true");
                response = await _httpClient.SendAsync(requestMessage);

                // If we used a cached redirect uri and it failed, we retry without using the cached uri
                if (usedRedirectUri && response.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    continue;
                }

             // Redirect must be handled manually in starrocks
            } while (response.StatusCode == System.Net.HttpStatusCode.RedirectKeepVerb);

            response.EnsureSuccessStatusCode();

            var transactionResponse = await response.Content.ReadFromJsonAsync<StreamLoadInfo>();
            if (transactionResponse == null)
            {
                throw new InvalidOperationException("Failed to parse stream load response.");   
            }
                
            if (transactionResponse.Status == "FAILED" ||
                transactionResponse.Status == "Fail")
            {
                throw new InvalidOperationException(transactionResponse.Message);
            }
        }

        public async Task<QueryResult> Query(string query)
        {
            var request = new QueryRequest(query);
            var response = await _httpClient.PostAsJsonAsync($"{_url}/api/v1/catalogs/default_catalog/sql", request);
            response.EnsureSuccessStatusCode();

            return await ResponseParser.ParseQuery(await response.Content.ReadAsStreamAsync().ConfigureAwait(false)).ConfigureAwait(false);
        }

        public async Task<TransactionInfo> CreateTransaction(string database, string table, string label)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_backendUrl}/api/transaction/begin");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", label);
            request.Headers.Add("db", database);
            request.Headers.Add("table", table);
            var response = await _httpClient.SendAsync(request); // ($"{_url}/api/{database}/{table}/transactions", request);
            response.EnsureSuccessStatusCode();

            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();

            if (transactionInfo == null)
            {
                throw new InvalidOperationException("Failed to parse transaction response.");
            }

            return transactionInfo;
        }

        public async Task TransactionLoad(string  database, string table, string label, ReadOnlyMemory<byte> data)
        {
            var request = new HttpRequestMessage(HttpMethod.Put, $"{_backendUrl}/api/transaction/load");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", label);
            request.Headers.Add("db", database);
            request.Headers.Add("table", table);
            request.Headers.Add("format", "json");
            request.Headers.Add("strip_outer_array", "true");
            var content = new ReadOnlyMemoryContent(data);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            request.Content = content;

            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();

            if (transactionInfo == null)
            {
                throw new InvalidOperationException("Failed to parse transaction response.");
            }

            if (transactionInfo.Status != "OK")
            {
                throw new InvalidOperationException(transactionInfo.Message);
            }
        }

        public async Task<TransactionInfo> TransactionPrepare(string database, string table, string label)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_backendUrl}/api/transaction/prepare");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", label);
            request.Headers.Add("db", database);
            request.Headers.Add("table", table);
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();
            if (transactionInfo == null)
            {
                throw new InvalidOperationException("Failed to parse transaction response.");
            }
            return transactionInfo;
        }

        public async Task<StreamLoadInfo> TransactionCommit(string database, string table, string label)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_backendUrl}/api/transaction/commit");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", label);
            request.Headers.Add("db", database);
            request.Headers.Add("table", table);
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var transactionResponse = await response.Content.ReadFromJsonAsync<StreamLoadInfo>();
            if (transactionResponse == null)
            {
                throw new InvalidOperationException("Failed to parse stream load response.");
            }

            return transactionResponse;
        }

        public async Task<TransactionInfo> TransactionRollback(string database, string table, string label)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_backendUrl}/api/transaction/rollback");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", label);
            request.Headers.Add("db", database);
            request.Headers.Add("table", table);
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();
            if (transactionInfo == null)
            {
                throw new InvalidOperationException("Failed to parse transaction response.");
            }
            return transactionInfo;
        }

        public Task<TableInfo> GetTableInfo(List<string> names)
        {
            return StarrocksUtils.GetTableInfo(_options, names);
        }
    }
}
