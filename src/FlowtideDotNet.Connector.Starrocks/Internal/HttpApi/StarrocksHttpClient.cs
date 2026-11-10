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

using FlowtideDotNet.Connector.StarRocks.Exceptions;
using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    internal class StarRocksHttpClient : IStarrocksClient
    {
        private static readonly TimeSpan ErrorLogTimeout = TimeSpan.FromSeconds(10);
        // Enough rejected rows to show the cause
        private const int MaxErrorLogLines = 10;
        private const int MaxErrorLogChars = 4096;

        private readonly string _url;
        private readonly string _backendUrl;
        private readonly HttpClient _httpClient;
        private readonly StarRocksSinkOptions _options;
        // Parallel loads share this cache
        private readonly ConcurrentDictionary<string, string> _redirectUris;

        public StarRocksHttpClient(StarRocksSinkOptions options)
        {
            _url = options.HttpUrl;
            _backendUrl = _url;

            if (options.BackendHttpUrl != null)
            {
                _backendUrl = options.BackendHttpUrl;
            }
            _redirectUris = new ConcurrentDictionary<string, string>();
            _httpClient = new HttpClient(new StarRocksHttpClientHandler() { AllowAutoRedirect = false })
            {
            };

            _options = options;
        }

        private AuthenticationHeaderValue GetAuthorizationHeaderValue()
        {
            var credentials = _options.Credentials();
            var authenticationString = $"{credentials.Username}:{credentials.Password}";
            var base64EncodedAuthenticationString = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(authenticationString));
            return new AuthenticationHeaderValue("Basic", base64EncodedAuthenticationString);
        }

        public async Task StreamLoad(StarRocksStreamLoadInfo request)
        {
            HttpResponseMessage? response = default;

            bool triedCachedRedirect = false;
            do
            {
                string? requestUrl = $"{_backendUrl}/api/{request.database}/{request.table}/_stream_load";
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
                        throw new StarRocksHttpException("Redirect response missing Location header.");
                    }
                    var newUrl = response.Headers.Location.ToString();
                    // Store the new redirect uri for future requests
                    _redirectUris[requestUrl] = newUrl;
                    requestUrl = newUrl;
                }

                var content = new ReadOnlyMemoryContent(request.data);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                using var requestMessage = new HttpRequestMessage(HttpMethod.Put, requestUrl)
                {
                    Content = content
                };
                requestMessage.Headers.Add("Expect", "100-continue");
                requestMessage.Headers.Authorization = GetAuthorizationHeaderValue();
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
                throw new StarRocksHttpException("Failed to parse stream load response.");   
            }
                
            if (transactionResponse.Status == StarRocksStatus.Failed)
            {
                await AddErrorLog(transactionResponse);
                if (transactionResponse.HasRejectedRows)
                {
                    throw new StarRocksInvalidDataException(transactionResponse.GetErrorMessage());
                }
                throw new StarRocksHttpException(transactionResponse.GetErrorMessage());
            }
        }

        private async Task AddErrorLog(LoadResponse response)
        {
            if (response.Status == StarRocksStatus.Ok || response.ErrorUrl == null)
            {
                return;
            }
            response.ErrorLog = await FetchErrorLog(response.ErrorUrl);
        }

        private async Task<string?> FetchErrorLog(string errorUrl)
        {
            if (!Uri.TryCreate(errorUrl, UriKind.Absolute, out var uri))
            {
                return null;
            }

            // BE may advertise an address only reachable internally
            var candidates = new List<string>();
            if (_options.BackendHttpUrl != null)
            {
                candidates.Add($"{_backendUrl}{uri.PathAndQuery}");
            }
            candidates.Add(errorUrl);

            foreach (var candidate in candidates)
            {
                try
                {
                    using var timeout = new CancellationTokenSource(ErrorLogTimeout);
                    using var response = await _httpClient.GetAsync(candidate, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
                    // Missing log is also 200, but html
                    if (!response.IsSuccessStatusCode || response.Content.Headers.ContentType?.MediaType != "text/plain")
                    {
                        continue;
                    }
                    using var stream = await response.Content.ReadAsStreamAsync(timeout.Token);
                    var log = await ReadErrorLogHead(stream, timeout.Token);
                    if (log != null)
                    {
                        return log;
                    }
                }
                catch (Exception)
                {
                    // Best effort, never hide the load error
                }
            }
            return null;
        }

        private static async Task<string?> ReadErrorLogHead(Stream stream, CancellationToken cancellationToken)
        {
            using var reader = new StreamReader(stream);
            var builder = new StringBuilder();
            int lineCount = 0;
            string? line;
            while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
            {
                if (line.Length == 0)
                {
                    continue;
                }
                if (lineCount == MaxErrorLogLines || builder.Length >= MaxErrorLogChars)
                {
                    builder.AppendLine();
                    builder.Append("...");
                    break;
                }
                var remaining = MaxErrorLogChars - builder.Length;
                if (line.Length > remaining)
                {
                    line = string.Concat(line.AsSpan(0, remaining), "...");
                }
                if (lineCount > 0)
                {
                    builder.AppendLine();
                }
                builder.Append(line);
                lineCount++;
            }
            return builder.Length == 0 ? null : builder.ToString();
        }

        public async Task<QueryResult> Query(string query)
        {
            var request = new QueryRequest(query);
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, $"{_url}/api/v1/catalogs/default_catalog/sql");
            requestMessage.Headers.Authorization = GetAuthorizationHeaderValue();
            requestMessage.Content = JsonContent.Create(request);
            var response = await _httpClient.SendAsync(requestMessage);
            response.EnsureSuccessStatusCode();

            return await ResponseParser.ParseQuery(await response.Content.ReadAsStreamAsync().ConfigureAwait(false)).ConfigureAwait(false);
        }

        public async Task<TransactionInfo> CreateTransaction(StarRocksTransactionId transactionId)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_backendUrl}/api/transaction/begin");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", transactionId.label);
            request.Headers.Add("db", transactionId.database);
            request.Headers.Add("table", transactionId.table);
            request.Headers.Authorization = GetAuthorizationHeaderValue();
            var response = await _httpClient.SendAsync(request); // ($"{_url}/api/{database}/{table}/transactions", request);
            response.EnsureSuccessStatusCode();

            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();

            if (transactionInfo == null)
            {
                throw new StarRocksTransactionException("Failed to parse transaction response.");
            }

            return transactionInfo;
        }

        public async Task TransactionLoad(StarRocksTransactionLoadInfo request)
        {
            var httpRequest = new HttpRequestMessage(HttpMethod.Put, $"{_backendUrl}/api/transaction/load");
            httpRequest.Headers.Add("Expect", "100-continue");
            httpRequest.Headers.Add("label", request.label);
            httpRequest.Headers.Add("db", request.database);
            httpRequest.Headers.Add("table", request.table);
            httpRequest.Headers.Add("format", "json");
            httpRequest.Headers.Add("strip_outer_array", "true");
            httpRequest.Headers.Add("ignore_json_size", "true");
            httpRequest.Headers.Authorization = GetAuthorizationHeaderValue();
            var content = new ReadOnlyMemoryContent(request.data);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            httpRequest.Content = content;

            var response = await _httpClient.SendAsync(httpRequest);
            response.EnsureSuccessStatusCode();

            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();

            if (transactionInfo == null)
            {
                throw new StarRocksTransactionException("Failed to parse transaction response.");
            }

            if (transactionInfo.Status != StarRocksStatus.Ok)
            {
                await AddErrorLog(transactionInfo);
                if (transactionInfo.HasRejectedRows)
                {
                    throw new StarRocksInvalidDataException(transactionInfo.GetErrorMessage());
                }
                throw new StarRocksTransactionException(transactionInfo.GetErrorMessage());
            }
        }

        public async Task<TransactionInfo> TransactionPrepare(StarRocksTransactionId transactionId)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_backendUrl}/api/transaction/prepare");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", transactionId.label);
            request.Headers.Add("db", transactionId.database);
            request.Headers.Add("table", transactionId.table);
            request.Headers.Authorization = GetAuthorizationHeaderValue();
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();
            if (transactionInfo == null)
            {
                throw new StarRocksTransactionException("Failed to parse transaction response.");
            }
            // Filtered rows are rejected at prepare
            await AddErrorLog(transactionInfo);
            return transactionInfo;
        }

        public async Task<StreamLoadInfo> TransactionCommit(StarRocksTransactionId transactionId)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_url}/api/transaction/commit");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", transactionId.label);
            request.Headers.Add("db", transactionId.database);
            request.Headers.Add("table", transactionId.table);
            request.Headers.Authorization = GetAuthorizationHeaderValue();
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var transactionResponse = await response.Content.ReadFromJsonAsync<StreamLoadInfo>();
            if (transactionResponse == null)
            {
                throw new StarRocksTransactionException("Failed to parse stream load response.");
            }

            await AddErrorLog(transactionResponse);
            return transactionResponse;
        }

        public async Task<TransactionInfo> TransactionRollback(StarRocksTransactionId transactionId)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"{_backendUrl}/api/transaction/rollback");
            request.Headers.Add("Expect", "100-continue");
            request.Headers.Add("label", transactionId.label);
            request.Headers.Add("db", transactionId.database);
            request.Headers.Add("table", transactionId.table);
            request.Headers.Authorization = GetAuthorizationHeaderValue();
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
            var transactionInfo = await response.Content.ReadFromJsonAsync<TransactionInfo>();
            if (transactionInfo == null)
            {
                throw new StarRocksTransactionException("Failed to parse transaction response.");
            }
            return transactionInfo;
        }

        public Task<TableInfo> GetTableInfo(List<string> names)
        {
            return StarRocksUtils.GetTableInfo(_options, names);
        }
    }
}
