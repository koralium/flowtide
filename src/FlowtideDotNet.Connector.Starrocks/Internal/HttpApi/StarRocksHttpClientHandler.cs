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

using FlowtideDotNet.Connector.Starrocks.Exceptions;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    internal class StarRocksHttpClientHandler : HttpClientHandler
    {
        private ConcurrentDictionary<string, string> _redirectUriCache = new ConcurrentDictionary<string, string>();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri == null)
            {
                throw new ArgumentNullException(nameof(request.RequestUri));
            }

            var startUrl = request.RequestUri.ToString();


            bool triedCachedRedirect = false;
            HttpResponseMessage? response = default;
            do
            {
                bool usedRedirectUri = false;
                var requestUri = startUrl;

                if (!triedCachedRedirect && _redirectUriCache.TryGetValue(requestUri, out var redirectedUrl))
                {
                    requestUri = redirectedUrl;
                    triedCachedRedirect = true;
                    usedRedirectUri = true;
                }
                else if (response != null)
                {
                    if (response.Headers.Location == null)
                    {
                        throw new StarRocksHttpException("Redirect response missing Location header.");
                    }
                    var newUrl = response.Headers.Location.ToString();
                    // Store the new redirect uri for future requests
                    _redirectUriCache[requestUri] = newUrl;
                    requestUri = newUrl;
                }

                if (usedRedirectUri || response != null)
                {
                    request.RequestUri = new Uri(requestUri);
                }

                response = await base.SendAsync(request, cancellationToken);

                if (usedRedirectUri && response.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    continue;
                }
                if (response.StatusCode != System.Net.HttpStatusCode.RedirectKeepVerb)
                {
                    break;
                }

                var newRequest = new HttpRequestMessage(request.Method, request.RequestUri);
                newRequest.Content = request.Content;
                foreach (var header in request.Headers)
                {
                    newRequest.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }
            } while (response.StatusCode == System.Net.HttpStatusCode.RedirectKeepVerb);

            return response;
        }
    }
}
