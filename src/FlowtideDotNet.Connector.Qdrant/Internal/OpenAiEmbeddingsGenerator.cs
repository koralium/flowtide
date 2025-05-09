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
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.Qdrant.Internal
{
    /// <summary>
    /// Todo: how to register this, options? factory func in options?
    /// other type of factory?
    /// 
    /// Can we get a http client factory into this?
    /// </summary>
    public class OpenAiEmbeddingsGenerator : IEmbeddingGenerator
    {
        private readonly OpenAiEmbeddingOptions _options;
        private static readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        public OpenAiEmbeddingsGenerator(OpenAiEmbeddingOptions options)
        {
            _options = options;
        }

        public async ValueTask<float[]> GenerateEmbeddingAsync(string text, CancellationToken cancellationToken = default)
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Add("api-key", _options.ApiKeyFunc());

            var body = new StringContent($$"""{"input": "{{text}}" }""", Encoding.UTF8, "application/json");

            var result = await client
                .PostAsync(_options.UrlFunc(), body, cancellationToken)
                .ExecutePipeline(_options.ResiliencePipeline);

            result.EnsureSuccessStatusCode();
            var response = await result.Content.ReadFromJsonAsync<EmbeddingResponseRoot>(_serializerOptions, cancellationToken: cancellationToken);

            Debug.Assert(response != null);
            Debug.Assert(response.Data.Length == 1);

            return response.Data[0].Embedding;
        }

        internal sealed class EmbeddingResponseRoot
        {
            public required EmbeddingData[] Data { get; set; }
            public required string Model { get; set; }
            /// <summary>
            /// todo: Usage might be a nice metric to expose?
            /// could also be useful to use ensure we are below the limit of the model
            /// </summary>
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