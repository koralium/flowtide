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

using System.Diagnostics;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.Qdrant.Internal
{
    public class OpenAiEmbeddingsGenerator : IEmbeddingGenerator
    {
        private readonly OpenAiEmbeddingOptions _options;
        private static readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        private readonly HttpClient _client;

        public OpenAiEmbeddingsGenerator(OpenAiEmbeddingOptions options)
        {
            _options = options;
            _client = new HttpClient();
        }

        public async ValueTask<float[]> GenerateEmbeddingAsync(string text, CancellationToken cancellationToken = default)
        {
            var message = GetRequestMessage(text);

            var result = await _client
                .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ExecutePipeline(_options.ResiliencePipeline);

            if (!result.IsSuccessStatusCode)
            {
                try
                {
                    var body = await result.Content.ReadAsStringAsync(cancellationToken);
                    throw new OpenAiEmbeddingsGeneratorException(result.StatusCode, body);
                }
                catch (Exception ex)
                {
                    throw new OpenAiEmbeddingsGeneratorException(result.StatusCode, ex.Message);
                }
            }

            var response = await result.Content.ReadFromJsonAsync<EmbeddingResponseRoot>(_serializerOptions, cancellationToken: cancellationToken);

            Debug.Assert(response != null);
            Debug.Assert(response.Data.Length == 1);

            return response.Data[0].Embedding;
        }

        private HttpRequestMessage GetRequestMessage(string text)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, _options.UrlFunc());
            request.Headers.Add("api-key", _options.ApiKeyFunc());

            var body = JsonContent.Create(new
            {
                input = text
            }, new MediaTypeHeaderValue("application/json"));

            request.Content = body;
            return request;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            _client.Dispose();
        }

        internal sealed class EmbeddingResponseRoot
        {
            public required EmbeddingData[] Data { get; set; }
            public required string Model { get; set; }
            public required Usage Usage { get; set; }
        }

        internal sealed class Usage
        {
            [JsonPropertyName("prompt_tokens")]
            public required int PromptTokens { get; set; }
            [JsonPropertyName("total_tokens")]
            public required int TotalTokens { get; set; }
        }

        internal sealed class EmbeddingData
        {
            public int Index { get; set; }
            public required float[] Embedding { get; set; }
        }
    }
}