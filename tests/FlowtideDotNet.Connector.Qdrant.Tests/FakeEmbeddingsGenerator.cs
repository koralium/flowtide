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
    public class FakeEmbeddingsGenerator : IEmbeddingGenerator
    {
        public int Size { get; set; } = 4;

        public void Dispose()
        {
        }

        public ValueTask<float[]> GenerateEmbeddingAsync(string text, CancellationToken cancellationToken = default)
        {
            return new ValueTask<float[]>(
                [.. Enumerable.Range(0, Size)
                .Select(s =>
                    (float)Random.Shared.NextDouble()
                    )]
                );
        }
    }
}
