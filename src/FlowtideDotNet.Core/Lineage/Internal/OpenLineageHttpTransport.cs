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

using System.Text;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class OpenLineageHttpTransport : IOpenLineageTransport
    {
        private readonly OpenLineageHttpOptions _openLineageOptions;
        private readonly HttpClient _httpClient;
        private readonly string _url;

        public OpenLineageHttpTransport(OpenLineageHttpOptions openLineageOptions)
        {
            if (openLineageOptions.Url == null)
            {
                throw new ArgumentException("OpenLineageOptions.Url must be set");
            }
            _url = openLineageOptions.Url;
            _openLineageOptions = openLineageOptions;
            _httpClient = new HttpClient();
        }

        public async Task EmitAsync(OpenLineageTransportEvent lineageEvent, CancellationToken cancellationToken)
        {
            using var content = new StringContent(lineageEvent.Json, Encoding.UTF8, "application/json");
            using var message = new HttpRequestMessage(HttpMethod.Post, _url) { Content = content };
            if (_openLineageOptions.OnRequest != null)
            {
                _openLineageOptions.OnRequest(message);
            }
            using var response = await _httpClient.SendAsync(message, cancellationToken);
            response.EnsureSuccessStatusCode();
        }

        public void Dispose()
        {
            _httpClient.Dispose();
        }
    }
}
