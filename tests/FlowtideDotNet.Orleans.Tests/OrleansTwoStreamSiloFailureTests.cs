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
    /// Two independent streams run across two silos when one silo is stopped. Both streams
    /// must recover on the remaining silo without interfering with each other: the recovery
    /// of one stream involves rollbacks and replays that must never touch the grains, queues
    /// or results of the other stream. Uses its own cluster fixture since the cluster loses
    /// a silo permanently.
    /// </summary>
    public class OrleansTwoStreamSiloFailureTests : IClassFixture<OrleansTwoSiloClusterFixture>
    {
        private readonly OrleansTwoSiloClusterFixture _fixture;

        public OrleansTwoStreamSiloFailureTests(OrleansTwoSiloClusterFixture fixture)
        {
            _fixture = fixture;
        }

        private static string JoinSql(string prefix)
        {
            return $@"
            CREATE TABLE {prefix}_left (val any);
            CREATE TABLE {prefix}_right (val any);

            INSERT INTO {prefix}_out
            SELECT l.val FROM {prefix}_left l
            INNER JOIN {prefix}_right r ON l.val = r.val;
            ";
        }

        [Fact]
        public async Task BothStreamsRecoverWhenSiloStops()
        {
            var sqlA = JoinSql("dualfail_a");
            var sqlB = JoinSql("dualfail_b");
            TestTableStore.AddRows("dualfail_a_left", Enumerable.Range(0, 200).Select(x => (long)x));
            TestTableStore.AddRows("dualfail_a_right", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("dualfail_b_left", Enumerable.Range(0, 200).Select(x => (long)x));
            TestTableStore.AddRows("dualfail_b_right", Enumerable.Range(0, 60).Select(x => (long)x));

            var streamA = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_dualfail_a");
            var streamB = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_dualfail_b");
            await streamA.StartStreamAsync(new StartStreamRequest(sqlA, substreamCount: 2));
            await streamB.StartStreamAsync(new StartStreamRequest(sqlB, substreamCount: 2));

            await WaitForResult("dualfail_a_out", Enumerable.Range(0, 100).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));
            await WaitForResult("dualfail_b_out", Enumerable.Range(0, 60).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            // Stop the secondary silo, grains of both streams may be hosted there and must
            // recover on the remaining silo.
            await _fixture.Cluster.StopSiloAsync(_fixture.Cluster.SecondarySilos[0]);

            // Both streams must process data added after the silo loss.
            TestTableStore.AddRows("dualfail_a_right", Enumerable.Range(100, 50).Select(x => (long)x));
            TestTableStore.AddRows("dualfail_b_right", Enumerable.Range(60, 40).Select(x => (long)x));

            await WaitForResult("dualfail_a_out", Enumerable.Range(0, 150).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(240));
            await WaitForResult("dualfail_b_out", Enumerable.Range(0, 100).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(240));

            // Both streams still stop cleanly after the recovery.
            var stopA = streamA.StopStreamAsync(new StopStreamRequest(sqlA, substreamCount: 2));
            var stopB = streamB.StopStreamAsync(new StopStreamRequest(sqlB, substreamCount: 2));
            var both = Task.WhenAll(stopA, stopB);
            var finished = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(120)));
            Assert.True(finished == both, "Stopping the streams after the silo failure timed out");
            await both;
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
            var dumpPath = SharedRingBufferLogger.Dump($"dualfail_failure_{DateTime.UtcNow:HHmmss}.log");
            Assert.Fail($"Sink {sink} did not produce the expected result, expected {expected.Count} rows, got {result?.Count.ToString() ?? "null"} rows. Log dump: {dumpPath}");
        }
    }
}
