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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal class AccessTokenCacheProvider : TokenCredential
    {
        private readonly TokenCredential tokenCredential;
        private readonly ConcurrentDictionary<string, AccessToken> accessTokens = new ConcurrentDictionary<string, AccessToken>();

        public AccessTokenCacheProvider(TokenCredential tokenCredential)
        {
            this.tokenCredential = tokenCredential;
        }
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            var scopes = string.Join(" ", requestContext.Scopes);
            if (accessTokens.TryGetValue(scopes, out var cachedToken) && cachedToken.ExpiresOn > DateTimeOffset.UtcNow.Add(TimeSpan.FromMinutes(10)))
            {
                return cachedToken;
            }
            var fetchTokenTask = GetTokenAsync(requestContext, cancellationToken);
            if (fetchTokenTask.IsCompleted)
            {
                return fetchTokenTask.Result;
            }
            return fetchTokenTask.GetAwaiter().GetResult();
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            var scopes = string.Join(" ", requestContext.Scopes);
            if (accessTokens.TryGetValue(scopes, out var cachedToken) && cachedToken.ExpiresOn > DateTimeOffset.UtcNow.Add(TimeSpan.FromMinutes(10)))
            {
                return ValueTask.FromResult(cachedToken);
            }

            return GetTokenAsync_Slow(requestContext, cancellationToken);
        }

        private async ValueTask<AccessToken> GetTokenAsync_Slow(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            var scopes = string.Join(" ", requestContext.Scopes);
            var token = await tokenCredential.GetTokenAsync(requestContext, cancellationToken);
            accessTokens.AddOrUpdate(scopes, token, (key, oldValue) => token);
            return token;
        }
    }
}
