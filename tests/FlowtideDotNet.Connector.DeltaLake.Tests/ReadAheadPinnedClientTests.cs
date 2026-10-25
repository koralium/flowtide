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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class ReadAheadPinnedClientTests
    {
        // Qualifies the depth Azure reads ahead at: one pinned client serves many reads at once
        [Fact]
        public async Task PinnedAzureClientReadsConcurrently()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var storage = server.CreateStorage();
            var bodies = Enumerable.Range(0, 64).Select(i => Encoding.UTF8.GetBytes($"{{\"object\":{i}}}" + new string((char)('a' + i % 26), 1000 + 37 * i))).ToArray();
            for (int i = 0; i < bodies.Length; i++)
            {
                server.Put($"t/_delta_log/{i:D20}.json", bodies[i]);
            }

            for (int iteration = 0; iteration < 10; iteration++)
            {
                var reads = await Task.WhenAll(Enumerable.Range(0, bodies.Length).Select(async i =>
                {
                    using var stream = await storage.OpenRead($"/c/t/_delta_log/{i:D20}.json");
                    Assert.NotNull(stream);
                    using var copy = new MemoryStream();
                    await stream.CopyToAsync(copy);
                    return copy.ToArray();
                }));
                for (int i = 0; i < bodies.Length; i++)
                {
                    Assert.Equal(bodies[i], reads[i]);
                }
            }
        }
    }
}
