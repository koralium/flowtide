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
    /// End to end tests that run distributed streams as Orleans grains through the test
    /// cluster, the substreams exchange data with grain calls.
    /// </summary>
    public class OrleansStreamTests : IClassFixture<OrleansClusterFixture>
    {
        private readonly OrleansClusterFixture _fixture;

        public OrleansStreamTests(OrleansClusterFixture fixture)
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
            Assert.Fail($"Sink {sink} did not produce the expected result, expected [{string.Join(",", expected)}], got [{string.Join(",", result ?? new List<long>())}]");
        }

        /// <summary>
        /// A normal join query split automatically into two substreams runs as grains,
        /// produces the correct result including data added after the start, and stops
        /// gracefully through the stream grain.
        /// </summary>
        [Fact]
        public async Task AutoDistributedStreamProducesResultsAndStops()
        {
            var sql = JoinSql("s1");
            TestTableStore.AddRows("s1_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("s1_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_test_1");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("s1_out", expected, TimeSpan.FromSeconds(60));

            // Data added while the stream is running must flow through the substreams
            TestTableStore.AddRows("s1_right", Enumerable.Range(50, 25).Select(x => (long)x));
            expected = Enumerable.Range(0, 75).Select(x => (long)x).ToList();
            await WaitForResult("s1_out", expected, TimeSpan.FromSeconds(60));

            // The coordinated stop drains the substreams and completes
            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the stream through the stream grain timed out");
            await stopTask;
        }

        /// <summary>
        /// Two different streams with two substreams each run in the same cluster without
        /// interfering, stopping one stream does not disturb the other.
        /// </summary>
        [Fact]
        public async Task TwoStreamsRunIndependently()
        {
            var sqlA = JoinSql("iso_a");
            var sqlB = JoinSql("iso_b");
            TestTableStore.AddRows("iso_a_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("iso_a_right", Enumerable.Range(0, 40).Select(x => (long)x));
            TestTableStore.AddRows("iso_b_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("iso_b_right", Enumerable.Range(0, 60).Select(x => (long)x));

            var streamA = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_iso_a");
            var streamB = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_iso_b");
            await streamA.StartStreamAsync(new StartStreamRequest(sqlA, substreamCount: 2));
            await streamB.StartStreamAsync(new StartStreamRequest(sqlB, substreamCount: 2));

            var expectedA = Enumerable.Range(0, 40).Select(x => (long)x).ToList();
            var expectedB = Enumerable.Range(0, 60).Select(x => (long)x).ToList();
            await WaitForResult("iso_a_out", expectedA, TimeSpan.FromSeconds(60));
            await WaitForResult("iso_b_out", expectedB, TimeSpan.FromSeconds(60));

            // Stop stream A, stream B must keep processing new data afterwards
            var stopTask = streamA.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping stream A timed out");
            await stopTask;

            TestTableStore.AddRows("iso_b_right", Enumerable.Range(60, 20).Select(x => (long)x));
            expectedB = Enumerable.Range(0, 80).Select(x => (long)x).ToList();
            await WaitForResult("iso_b_out", expectedB, TimeSpan.FromSeconds(60));

            // Stream A's result stays at its final state from before the stop
            var resultA = TestTableStore.GetResult("iso_a_out");
            Assert.NotNull(resultA);
            Assert.Equal(expectedA, resultA);
        }

        /// <summary>
        /// A stopped stream can be started again with the same SQL and produces complete
        /// results including data that arrived while it was stopped. The stop cleared the
        /// grain state, so this verifies the whole start-stop-start grain lifecycle and the
        /// substream initialize handshake on the second start.
        /// </summary>
        [Fact]
        public async Task StreamRestartsAndProducesCompleteResults()
        {
            var sql = JoinSql("restart");
            TestTableStore.AddRows("restart_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("restart_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_restart");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("restart_out", expected, TimeSpan.FromSeconds(60));

            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the stream before the restart timed out");
            await stopTask;

            // Data arriving while the stream is stopped must show up after the restart
            TestTableStore.AddRows("restart_right", Enumerable.Range(50, 25).Select(x => (long)x));

            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            expected = Enumerable.Range(0, 75).Select(x => (long)x).ToList();
            await WaitForResult("restart_out", expected, TimeSpan.FromSeconds(60));

            // The restarted stream keeps processing live changes and stops cleanly again
            TestTableStore.AddRows("restart_right", Enumerable.Range(75, 25).Select(x => (long)x));
            expected = Enumerable.Range(0, 100).Select(x => (long)x).ToList();
            await WaitForResult("restart_out", expected, TimeSpan.FromSeconds(60));

            stopTask = streamGrain.StopStreamAsync();
            finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the restarted stream timed out");
            await stopTask;
        }

        /// <summary>
        /// A stop through the stream grain lands while a substream grain is running a fail
        /// and recover in the background. The grain acknowledges the recovery request
        /// immediately and recovers asynchronously, so the coordinated stop races the whole
        /// recovery including the restart handshake between the grains. The stop must
        /// complete and a restart afterwards must produce complete results.
        /// </summary>
        [Fact]
        public async Task StopDuringGrainRecoveryCompletesAndRestarts()
        {
            var sql = JoinSql("stoprec");
            TestTableStore.AddRows("stoprec_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("stoprec_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_stoprec");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("stoprec_out", expected, TimeSpan.FromSeconds(60));

            // Force a recovery in one substream grain, the request is acknowledged
            // immediately and the recovery runs in the background on the grain.
            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>("orleans_stoprec_substream_0");
            await substreamGrain.FailAndRecoverAsync(new FailAndRecoverRequest("substream_1", 0));

            // Stop while the recovery runs.
            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(90)));
            Assert.True(finished == stopTask, "Stopping the stream during a grain recovery timed out");
            await stopTask;

            // Data added while stopped and a restart must produce the complete result.
            TestTableStore.AddRows("stoprec_right", Enumerable.Range(50, 25).Select(x => (long)x));
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            expected = Enumerable.Range(0, 75).Select(x => (long)x).ToList();
            await WaitForResult("stoprec_out", expected, TimeSpan.FromSeconds(60));

            var finalStop = streamGrain.StopStreamAsync();
            finished = await Task.WhenAny(finalStop, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == finalStop, "The final stop timed out");
            await finalStop;
        }

        /// <summary>
        /// A fail and recover request lands while the coordinated stop is already draining
        /// the substreams, the reverse ordering of StopDuringGrainRecoveryCompletesAndRestarts.
        /// The recovery interrupts the stop drain in one grain while the other grain waits
        /// for its stop barrier to be consumed. The stop must still complete and a restart
        /// must produce complete results.
        /// </summary>
        [Fact]
        public async Task RecoveryDuringCoordinatedStopStillStops()
        {
            var sql = JoinSql("recstop");
            TestTableStore.AddRows("recstop_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("recstop_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_recstop");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("recstop_out", expected, TimeSpan.FromSeconds(60));

            // Start the coordinated stop and inject a recovery while it drains
            var stopTask = streamGrain.StopStreamAsync();
            await Task.Delay(50);
            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>("orleans_recstop_substream_0");
            await substreamGrain.FailAndRecoverAsync(new FailAndRecoverRequest("substream_1", 0));

            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(90)));
            Assert.True(finished == stopTask, "The coordinated stop did not complete after a recovery landed mid drain");
            await stopTask;

            // A restart with new data must produce the complete result
            TestTableStore.AddRows("recstop_right", Enumerable.Range(50, 25).Select(x => (long)x));
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            expected = Enumerable.Range(0, 75).Select(x => (long)x).ToList();
            await WaitForResult("recstop_out", expected, TimeSpan.FromSeconds(60));

            var finalStop = streamGrain.StopStreamAsync();
            finished = await Task.WhenAny(finalStop, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == finalStop, "The final stop timed out");
            await finalStop;
        }

        /// <summary>
        /// Stop takes no arguments, EVERY substream grain that was started must be stopped
        /// from the persisted started set alone. Missing one would orphan a running grain
        /// whose keep alive reminder restarts it forever with no remaining way to stop it.
        /// A restart with a different substream count must then come up cleanly.
        /// </summary>
        [Fact]
        public async Task StopFromPersistedStateStopsAllStartedGrains()
        {
            var sql = JoinSql("mismatch");
            TestTableStore.AddRows("mismatch_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("mismatch_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_mismatch");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("mismatch_out", expected, TimeSpan.FromSeconds(60));

            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping from the persisted started set timed out");
            await stopTask;

            // A restart with yet another count must come up cleanly, an orphaned running
            // grain from the first start would collide with the new topology.
            TestTableStore.AddRows("mismatch_right", Enumerable.Range(50, 25).Select(x => (long)x));
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 3));

            expected = Enumerable.Range(0, 75).Select(x => (long)x).ToList();
            await WaitForResult("mismatch_out", expected, TimeSpan.FromSeconds(90));

            var finalStop = streamGrain.StopStreamAsync();
            finished = await Task.WhenAny(finalStop, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == finalStop, "The final stop timed out");
            await finalStop;
        }

        /// <summary>
        /// Rapid start and stop cycles through the grains, the stop lands while the
        /// substream streams are still starting, which exercises the grain lifecycle,
        /// reminder registration and the tick loop teardown under repetition. Every stop
        /// must complete and the final cycle must produce correct results.
        /// </summary>
        [Fact]
        public async Task RapidGrainStartStopCyclesComplete()
        {
            var sql = JoinSql("cycle");
            TestTableStore.AddRows("cycle_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("cycle_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_cycle");

            for (int cycle = 0; cycle < 3; cycle++)
            {
                await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));
                // Stop immediately, the substream streams are typically still starting.
                var cycleStop = streamGrain.StopStreamAsync();
                var cycleFinished = await Task.WhenAny(cycleStop, Task.Delay(TimeSpan.FromSeconds(60)));
                Assert.True(cycleFinished == cycleStop, $"Stopping the stream timed out in cycle {cycle}");
                await cycleStop;
            }

            // The final cycle runs to a full result and stops cleanly.
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));
            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("cycle_out", expected, TimeSpan.FromSeconds(90));

            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the final cycle timed out");
            await stopTask;
        }
    }
}
