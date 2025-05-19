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

using FlowtideDotNet.Connector.Qdrant.Internal;

namespace FlowtideDotNet.Connector.Qdrant.Tests
{
    public class EvenPartsChunker : IStringChunker
    {
        private readonly int _numberOfParts;

        public EvenPartsChunker(int numberOfParts)
        {
            _numberOfParts = numberOfParts;
        }

        public ValueTask<IReadOnlyList<string>> Chunk(string input, CancellationToken cancellationToken = default)
        {
            var result = new List<string>();
            int partSize = input.Length / _numberOfParts;
            int remainder = input.Length % _numberOfParts;

            int currentIndex = 0;
            for (int i = 0; i < _numberOfParts; i++)
            {
                int currentPartSize = partSize + (i < remainder ? 1 : 0);
                result.Add(input.Substring(currentIndex, currentPartSize));
                currentIndex += currentPartSize;
            }

            return ValueTask.FromResult<IReadOnlyList<string>>(result);
        }
    }

    public class EveryCharChunker : IStringChunker
    {
        public ValueTask<IReadOnlyList<string>> Chunk(string input, CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult<IReadOnlyList<string>>([.. input.Select(s => s.ToString())]);
        }
    }
}
