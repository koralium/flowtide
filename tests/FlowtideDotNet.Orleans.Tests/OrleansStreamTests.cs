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
            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(Internal.SubStreamGrainKey.Create("orleans_stoprec", "substream_0"));
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
            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(Internal.SubStreamGrainKey.Create("orleans_recstop", "substream_0"));
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
        /// Starting an already running stream with a different SQL text or substream count
        /// must throw. Silently accepting it would leave the existing substream grains
        /// running the old plan while substreams new in the request start the new one, a
        /// mixed topology. Starting with the identical request is an idempotent no-op, and
        /// after a stop a new plan starts normally.
        /// </summary>
        [Fact]
        public async Task StartWithChangedPlanWithoutStopThrows()
        {
            var sql = JoinSql("chg");
            TestTableStore.AddRows("chg_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("chg_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_changed_plan");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("chg_out", expected, TimeSpan.FromSeconds(60));

            // Different SQL text and different substream count are both rejected
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => streamGrain.StartStreamAsync(new StartStreamRequest(JoinSql("chg2"), substreamCount: 2)));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 3)));

            // The identical request is an idempotent no-op and the stream keeps running
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));
            TestTableStore.AddRows("chg_right", Enumerable.Range(50, 25).Select(x => (long)x));
            expected = Enumerable.Range(0, 75).Select(x => (long)x).ToList();
            await WaitForResult("chg_out", expected, TimeSpan.FromSeconds(60));

            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the stream timed out");
            await stopTask;

            // After the stop a changed plan starts normally
            TestTableStore.AddRows("chg2_left", Enumerable.Range(0, 30).Select(x => (long)x));
            TestTableStore.AddRows("chg2_right", Enumerable.Range(0, 10).Select(x => (long)x));
            await streamGrain.StartStreamAsync(new StartStreamRequest(JoinSql("chg2"), substreamCount: 2));
            await WaitForResult("chg2_out", Enumerable.Range(0, 10).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(60));

            var finalStop = streamGrain.StopStreamAsync();
            finished = await Task.WhenAny(finalStop, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == finalStop, "The final stop timed out");
            await finalStop;
        }

        /// <summary>
        /// The status call reports every started substream, their states become Running on
        /// a healthy stream, and a stopped stream reports not started with no substreams.
        /// This is the only way a caller can observe the stream actually running, the start
        /// call returns before the streams start in the background.
        /// </summary>
        [Fact]
        public async Task StatusReportsSubstreamsRunningAndStopped()
        {
            var sql = JoinSql("stat");
            TestTableStore.AddRows("stat_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("stat_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_status");

            // Before any start the stream reports not started
            var status = await streamGrain.GetStatusAsync();
            Assert.False(status.IsStarted);
            Assert.Empty(status.Substreams);

            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            // The substreams must become Running within the timeout
            var deadline = DateTime.UtcNow.AddSeconds(60);
            while (true)
            {
                status = await streamGrain.GetStatusAsync();
                Assert.True(status.IsStarted);
                Assert.Equal(2, status.Substreams.Count);
                Assert.All(status.Substreams, s => Assert.True(s.IsStarted));
                Assert.All(status.Substreams, s => Assert.Null(s.LastFailure));
                if (status.Substreams.All(s => s.State == FlowtideDotNet.Base.Engine.StreamStateValue.Running))
                {
                    break;
                }
                Assert.True(DateTime.UtcNow < deadline,
                    $"Substreams did not reach Running, states: [{string.Join(",", status.Substreams.Select(s => $"{s.SubstreamName}={s.State}"))}]");
                await Task.Delay(100);
            }
            Assert.Equal(
                new[] { "substream_0", "substream_1" },
                status.Substreams.Select(s => s.SubstreamName).OrderBy(x => x).ToArray());

            await WaitForResult("stat_out", Enumerable.Range(0, 50).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(60));

            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the stream timed out");
            await stopTask;

            status = await streamGrain.GetStatusAsync();
            Assert.False(status.IsStarted);
            Assert.Empty(status.Substreams);
        }

        /// <summary>
        /// Starting returns success before the streams start in the background, a stream
        /// that can never start, for example because a connector fails to initialize, must
        /// surface the failure through the status API since the start call cannot. The
        /// stream must also still stop cleanly.
        /// </summary>
        [Fact]
        public async Task BackgroundStartFailureSurfacesInStatus()
        {
            // Tables starting with poison fail source initialization, see TestConnectors.
            var sql = @"
            CREATE TABLE poison_left (val any);
            CREATE TABLE bsf_right (val any);

            INSERT INTO bsf_out
            SELECT l.val FROM poison_left l
            INNER JOIN bsf_right r ON l.val = r.val;
            ";

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_start_failure");
            // The start call itself succeeds, the streams fail in the background
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            var deadline = DateTime.UtcNow.AddSeconds(60);
            while (true)
            {
                var status = await streamGrain.GetStatusAsync();
                Assert.True(status.IsStarted);
                if (status.Substreams.Any(s => s.LastFailure != null && s.LastFailure.Contains("Poisoned")))
                {
                    break;
                }
                Assert.True(DateTime.UtcNow < deadline,
                    $"No substream reported the start failure, statuses: [{string.Join(",", status.Substreams.Select(s => $"{s.SubstreamName}={s.State} failure={s.LastFailure != null}"))}]");
                await Task.Delay(100);
            }

            // A stream whose substreams never started must still stop cleanly
            var stopTask = streamGrain.StopStreamAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the failed stream timed out");
            await stopTask;

            var stoppedStatus = await streamGrain.GetStatusAsync();
            Assert.False(stoppedStatus.IsStarted);
        }

        /// <summary>
        /// Deleting a stream stops all substreams, deletes their state, completes, and the
        /// stream reports not started afterwards. A fresh start with a different plan then
        /// works, which would be rejected if the delete had not cleared the stream record.
        /// </summary>
        [Fact]
        public async Task DeleteStreamRemovesStateAndAllowsFreshStart()
        {
            var sql = JoinSql("del");
            TestTableStore.AddRows("del_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("del_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_delete");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));
            await WaitForResult("del_out", Enumerable.Range(0, 50).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(60));

            var deleteTask = streamGrain.DeleteStreamAsync();
            var finished = await Task.WhenAny(deleteTask, Task.Delay(TimeSpan.FromSeconds(60)));
            if (finished != deleteTask)
            {
                var timeoutDump = SharedRingBufferLogger.Dump($"delete_timeout_{DateTime.UtcNow:HHmmss}.log");
                Assert.Fail($"Deleting the stream timed out. Log dump: {timeoutDump}");
            }
            try
            {
                await deleteTask;
            }
            catch (Exception e)
            {
                var failureDump = SharedRingBufferLogger.Dump($"delete_failure_{DateTime.UtcNow:HHmmss}.log");
                Assert.Fail($"Deleting the stream failed: {e}. Log dump: {failureDump}");
            }

            var status = await streamGrain.GetStatusAsync();
            Assert.False(status.IsStarted);
            Assert.Empty(status.Substreams);

            // A different plan starts fresh after the delete, proving the stream record and
            // the changed plan guard were cleared.
            TestTableStore.AddRows("del2_left", Enumerable.Range(0, 30).Select(x => (long)x));
            TestTableStore.AddRows("del2_right", Enumerable.Range(0, 10).Select(x => (long)x));
            await streamGrain.StartStreamAsync(new StartStreamRequest(JoinSql("del2"), substreamCount: 2));
            await WaitForResult("del2_out", Enumerable.Range(0, 10).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(60));

            var stopTask = streamGrain.StopStreamAsync();
            finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping after the delete timed out");
            await stopTask;
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
