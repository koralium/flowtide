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

using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Messages;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// Runs a distributed stream on a cluster with two silos. The substream grains are placed
    /// across the silos, so the fetches, checkpoints and coordination messages between them
    /// are serialized over the network like in a multi node deployment.
    /// </summary>
    public class OrleansTwoSiloStreamTests : IClassFixture<OrleansTwoSiloClusterFixture>
    {
        private readonly OrleansTwoSiloClusterFixture _fixture;

        public OrleansTwoSiloStreamTests(OrleansTwoSiloClusterFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task StreamRunsAcrossTwoSilos()
        {
            var sql = @"
            CREATE TABLE twosilo_left (val any);
            CREATE TABLE twosilo_right (val any);

            INSERT INTO twosilo_out
            SELECT l.val FROM twosilo_left l
            INNER JOIN twosilo_right r ON l.val = r.val;
            ";
            TestTableStore.AddRows("twosilo_left", Enumerable.Range(0, 200).Select(x => (long)x));
            TestTableStore.AddRows("twosilo_right", Enumerable.Range(0, 100).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_two_silo");
            // Four substreams so the grains spread across both silos.
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 4));

            var expected = Enumerable.Range(0, 100).Select(x => (long)x).ToList();
            await WaitForResult("twosilo_out", expected, TimeSpan.FromSeconds(90));

            // Data added while the stream runs must flow across the silos
            TestTableStore.AddRows("twosilo_right", Enumerable.Range(100, 50).Select(x => (long)x));
            expected = Enumerable.Range(0, 150).Select(x => (long)x).ToList();
            await WaitForResult("twosilo_out", expected, TimeSpan.FromSeconds(90));

            var stopTask = streamGrain.StopStreamAsync(new StopStreamRequest(sql, substreamCount: 4));
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(90)));
            Assert.True(finished == stopTask, "Stopping the two silo stream timed out");
            await stopTask;
        }

        private static async Task WaitForResult(string sink, List<long> expected, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow.Add(timeout);
            List<long>? result = null;
            while (DateTime.UtcNow < deadline)
            {
                result = TestTableStore.GetResult(sink);
                if (result != null && result.SequenceEqual(expected))
                {
                    return;
                }
                await Task.Delay(100);
            }
            Assert.Fail($"Sink {sink} did not produce the expected result, expected {expected.Count} rows, got [{string.Join(",", result ?? new List<long>())}]");
        }
    }
}
