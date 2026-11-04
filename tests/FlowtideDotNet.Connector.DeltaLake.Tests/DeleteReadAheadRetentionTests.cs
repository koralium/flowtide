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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeleteReadAheadRetentionTests
    {
        [Theory]
        [InlineData(1)]
        [InlineData(8)]
        public async Task ConsumedAzureBodiesDoNotStayRootedByTheWindow(int depth)
        {
            const int count = 9;
            var body = new byte[(int)CommitReadAhead.WindowBytes + 1];
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var storage = server.CreateStorage();
            var candidates = Enumerable.Range(0, count).ToList();
            var records = new Dictionary<int, DeltaFileRecord>();
            foreach (var id in candidates)
            {
                var path = $"data-{id}.parquet";
                server.Put($"t/{path}", body);
                records.Add(id, new DeltaFileRecord { Path = path, Size = body.Length });
            }
            await using var window = depth > 1
                ? CandidateReadAhead.Create(storage, "/c/t", candidates, 0, count, records, depth, CancellationToken.None)
                : null;
            var consumed = new List<WeakReference>();
            foreach (var id in candidates.Take(8))
            {
                consumed.Add(await TakeAndDispose(storage, window, $"/c/t/{records[id].Path}"));
                Assert.Equal(id + 1, server.Requests.Count(x => x.StartsWith("GET ", StringComparison.Ordinal)));
                Assert.Equal(0, window?.Outstanding ?? 0);
            }
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var retained = consumed.Count(x => x.IsAlive);
            GC.KeepAlive(window);
            Assert.True(retained <= 1, $"{retained} disposed Azure bodies remain rooted; depth {depth}, {body.Length} bytes per body");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static async Task<WeakReference> TakeAndDispose(IFileStorage storage, CommitReadAhead? window, string path)
        {
            var opened = window == null ? default : await window.NextAsync();
            var stream = window == null ? await storage.OpenRead(path) : opened.Stream;
            Assert.IsAssignableFrom<MemoryStream>(stream);
            var reference = new WeakReference(stream);
            stream!.Dispose();
            opened.Dispose();
            opened = default;
            stream = null;
            return reference;
        }
    }
}
