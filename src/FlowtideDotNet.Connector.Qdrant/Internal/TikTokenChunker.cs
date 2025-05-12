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

namespace FlowtideDotNet.Connector.Qdrant.Internal
{
    public class TikTokenChunker : IStringChunker
    {
        private readonly TikTokenChunkerOptions _options;

        public TikTokenChunker(TikTokenChunkerOptions options)
        {
            _options = options;
        }

        public ValueTask<IReadOnlyList<string>> Chunk(string input, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(input);

            var tokenizer = _options.Tokenizer;
            input = input.Replace("\n", "").Replace("\r", "");

            var tokens = tokenizer.EncodeToIds(input);

            var chunks = new List<string>();
            var index = 0;

            while (index < tokens.Count)
            {
                var remaining = tokens.Count - index;
                var count = Math.Min(_options.TokenChunkSize, remaining);

                var subTokens = tokens.Skip(index).Take(count);
                var chunkText = tokenizer.Decode(subTokens);

                var tooShort = chunkText.Length < _options.MinTokenChunkSize;

                // If the remaining tokens result in a short chunk, merge with previous
                if ((tooShort || remaining <= _options.TokenChunkOverlap) && chunks.Count > 0)
                {
                    // Append to previous chunk
                    var lastChunkTokens = tokenizer.EncodeToIds(chunks[^1]);
                    chunks[^1] = tokenizer.Decode(lastChunkTokens.Concat(subTokens));
                    break;
                }

                chunks.Add(chunkText);
                index += (_options.TokenChunkSize - _options.TokenChunkOverlap);
            }

            return ValueTask.FromResult<IReadOnlyList<string>>(chunks);
        }
    }
}