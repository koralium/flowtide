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

using FlowtideDotNet.Cluster.Orleans.Interfaces;
using FlowtideDotNet.Cluster.Orleans.Messages;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    /// <summary>
    /// Stops a silo while a distributed stream runs across the cluster. Uses its own cluster
    /// fixture since the cluster loses a silo permanently.
    /// </summary>
    public class OrleansSiloFailureTests : IClassFixture<OrleansTwoSiloClusterFixture>
    {
        private readonly OrleansTwoSiloClusterFixture _fixture;

        public OrleansSiloFailureTests(OrleansTwoSiloClusterFixture fixture)
        {
            _fixture = fixture;
        }

        /// <summary>
        /// When a silo shuts down, its substream grains deactivate, which stops their streams,
        /// and grain calls from the surviving substreams reactivate them on the remaining
        /// silo. The reactivated substreams resume from their stored grain state, the
        /// initialize handshake reconciles the checkpoint versions with a mutual rollback and
        /// the sources replay, so the results stay complete, including data added after the
        /// silo stopped.
        /// </summary>
        [Fact]
        public async Task StreamRecoversWhenSiloStops()
        {
            var sql = @"
            CREATE TABLE silostop_left (val any);
            CREATE TABLE silostop_right (val any);

            INSERT INTO silostop_out
            SELECT l.val FROM silostop_left l
            INNER JOIN silostop_right r ON l.val = r.val;
            ";
            TestTableStore.AddRows("silostop_left", Enumerable.Range(0, 200).Select(x => (long)x));
            TestTableStore.AddRows("silostop_right", Enumerable.Range(0, 100).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_silostop");
            // Four substreams so grains are spread over both silos.
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 4));

            var expected = Enumerable.Range(0, 100).Select(x => (long)x).ToList();
            await WaitForResult("silostop_out", expected, TimeSpan.FromSeconds(90));

            // Stop the secondary silo, the grains it hosted deactivate and reactivate on the
            // primary silo when the surviving substreams call them.
            await _fixture.Cluster.StopSiloAsync(_fixture.Cluster.SecondarySilos[0]);

            // The status call must survive the failure window: unreachable substream grains
            // become Error entries instead of failing the whole call, and every started
            // substream is always reported.
            var statusDuringRecovery = await streamGrain.GetStatusAsync();
            Assert.True(statusDuringRecovery.IsStarted);
            Assert.Equal(4, statusDuringRecovery.Substreams.Count);

            // Data added after the silo stopped must still reach the result. The recovery can
            // take a few minutes, grains whose activations died with the silo are reactivated
            // by the keep alive reminder, which fires at most a minute after the failure, and
            // by calls from the surviving substreams, which first fail with the response
            // timeout before the streams fail over and recover.
            TestTableStore.AddRows("silostop_right", Enumerable.Range(100, 50).Select(x => (long)x));
            expected = Enumerable.Range(0, 150).Select(x => (long)x).ToList();
            await WaitForResult("silostop_out", expected, TimeSpan.FromSeconds(240));

            // The recovered stream still stops cleanly.
            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(120)));
            Assert.True(finished == stopTask, "Stopping the stream after the silo failure timed out");
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
            var dumpPath = SharedRingBufferLogger.Dump($"silostop_failure_{DateTime.UtcNow:HHmmss}.log");
            Assert.Fail($"Sink {sink} did not produce the expected result, expected {expected.Count} rows, got {result?.Count.ToString() ?? "null"} rows. Log dump: {dumpPath}");
        }
    }
}
