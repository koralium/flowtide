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
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// A substream grain can be asked to migrate to another silo, which is what the runtime's
    /// activation rebalancing does to spread load. A migration is a planned, cooperative event
    /// while every silo is healthy - unlike a crash - so the stream must react to it without
    /// going through the crash recovery path: no substream should be failed and rolled back
    /// because a peer moved. Uses the durable storage fixture: the moved activation restores
    /// the state its predecessor persisted, without it a handoff has nothing to resume from.
    /// </summary>
    public class OrleansMigrationTests : IClassFixture<OrleansTwoSiloDurableStorageClusterFixture>
    {
        private readonly OrleansTwoSiloDurableStorageClusterFixture _fixture;

        public OrleansMigrationTests(OrleansTwoSiloDurableStorageClusterFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task MigratingASubstreamGrainDoesNotFailTheStream()
        {
            var sql = @"
            CREATE TABLE migrate_left (val any);
            CREATE TABLE migrate_right (val any);

            INSERT INTO migrate_out
            SELECT l.val FROM migrate_left l
            INNER JOIN migrate_right r ON l.val = r.val;
            ";
            TestTableStore.AddRows("migrate_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("migrate_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_migrate");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));

            // Data flows between the substreams before the migration, so the moved grain's
            // peer holds live exchanged state, the situation a rebalance hits in production.
            var expected = Enumerable.Range(0, 50).Select(x => (long)x).ToList();
            await WaitForResult("migrate_out", expected, TimeSpan.FromSeconds(90));

            // Migrate one substream's activation, as the activation rebalancer would.
            var substreamKey = SubStreamGrainKey.Create("orleans_migrate", "substream_0");
            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
            await MigrateAndWaitForNewActivation(substreamGrain);

            // Data added after the migration must still flow through both substreams.
            TestTableStore.AddRows("migrate_right", Enumerable.Range(50, 25).Select(x => (long)x));
            expected = Enumerable.Range(0, 75).Select(x => (long)x).ToList();
            await WaitForResult("migrate_out", expected, TimeSpan.FromSeconds(90));

            await AssertNoFailures(streamGrain);
            await StopStream(streamGrain);
        }

        /// <summary>
        /// The same substream can be migrated repeatedly: the announce flag is one-shot per
        /// handoff and the peer's resume tracking resets, so a second handoff must verify and
        /// resume as cleanly as the first.
        /// </summary>
        [Fact]
        public async Task MigratingTheSameSubstreamGrainTwiceDoesNotFailTheStream()
        {
            var sql = @"
            CREATE TABLE migrate_twice_left (val any);
            CREATE TABLE migrate_twice_right (val any);

            INSERT INTO migrate_twice_out
            SELECT l.val FROM migrate_twice_left l
            INNER JOIN migrate_twice_right r ON l.val = r.val;
            ";
            TestTableStore.AddRows("migrate_twice_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("migrate_twice_right", Enumerable.Range(0, 40).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_migrate_twice");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));
            await WaitForResult("migrate_twice_out", Enumerable.Range(0, 40).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(
                SubStreamGrainKey.Create("orleans_migrate_twice", "substream_0"));

            await MigrateAndWaitForNewActivation(substreamGrain);
            TestTableStore.AddRows("migrate_twice_right", Enumerable.Range(40, 30).Select(x => (long)x));
            await WaitForResult("migrate_twice_out", Enumerable.Range(0, 70).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            await MigrateAndWaitForNewActivation(substreamGrain);
            TestTableStore.AddRows("migrate_twice_right", Enumerable.Range(70, 30).Select(x => (long)x));
            await WaitForResult("migrate_twice_out", Enumerable.Range(0, 100).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            await AssertNoFailures(streamGrain);
            await StopStream(streamGrain);
        }

        /// <summary>
        /// Both substreams can be migrated one after the other, each side plays both roles:
        /// the moved substream that hands off and the peer that verifies and resumes.
        /// </summary>
        [Fact]
        public async Task MigratingBothSubstreamGrainsSequentiallyDoesNotFailTheStream()
        {
            var sql = @"
            CREATE TABLE migrate_both_left (val any);
            CREATE TABLE migrate_both_right (val any);

            INSERT INTO migrate_both_out
            SELECT l.val FROM migrate_both_left l
            INNER JOIN migrate_both_right r ON l.val = r.val;
            ";
            TestTableStore.AddRows("migrate_both_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("migrate_both_right", Enumerable.Range(0, 40).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_migrate_both");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));
            await WaitForResult("migrate_both_out", Enumerable.Range(0, 40).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            var substream0 = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(
                SubStreamGrainKey.Create("orleans_migrate_both", "substream_0"));
            var substream1 = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(
                SubStreamGrainKey.Create("orleans_migrate_both", "substream_1"));

            await MigrateAndWaitForNewActivation(substream0);
            TestTableStore.AddRows("migrate_both_right", Enumerable.Range(40, 30).Select(x => (long)x));
            await WaitForResult("migrate_both_out", Enumerable.Range(0, 70).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            await MigrateAndWaitForNewActivation(substream1);
            TestTableStore.AddRows("migrate_both_right", Enumerable.Range(70, 30).Select(x => (long)x));
            await WaitForResult("migrate_both_out", Enumerable.Range(0, 100).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            await AssertNoFailures(streamGrain);
            await StopStream(streamGrain);
        }

        /// <summary>
        /// A migration while data keeps arriving must neither fail the stream nor lose rows:
        /// the drain covers everything consumed before the stop, and rows arriving while the
        /// substream is moving are picked up by the restored instance.
        /// </summary>
        [Fact]
        public async Task MigratingASubstreamGrainWhileDataFlowsDoesNotFailTheStream()
        {
            var sql = @"
            CREATE TABLE migrate_load_left (val any);
            CREATE TABLE migrate_load_right (val any);

            INSERT INTO migrate_load_out
            SELECT l.val FROM migrate_load_left l
            INNER JOIN migrate_load_right r ON l.val = r.val;
            ";
            TestTableStore.AddRows("migrate_load_left", Enumerable.Range(0, 500).Select(x => (long)x));
            TestTableStore.AddRows("migrate_load_right", Enumerable.Range(0, 50).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_migrate_load");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 2));
            await WaitForResult("migrate_load_out", Enumerable.Range(0, 50).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            // Feed rows in small batches while the migration below is in progress.
            var insertTask = Task.Run(async () =>
            {
                for (int batch = 0; batch < 9; batch++)
                {
                    TestTableStore.AddRows("migrate_load_right", Enumerable.Range(50 + batch * 50, 50).Select(x => (long)x));
                    await Task.Delay(100);
                }
            });

            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(
                SubStreamGrainKey.Create("orleans_migrate_load", "substream_0"));
            await MigrateAndWaitForNewActivation(substreamGrain);

            await insertTask;
            await WaitForResult("migrate_load_out", Enumerable.Range(0, 500).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            await AssertNoFailures(streamGrain);
            await StopStream(streamGrain);
        }

        /// <summary>
        /// With three substreams the migrating grain hands off against TWO peers at once:
        /// both must consume its stop barrier before it stops, and both must independently
        /// verify and accept the clean reconnect. Also exercises the per-peer ack bookkeeping
        /// (acks are credited per target) together with the handoff.
        /// </summary>
        [Fact]
        public async Task MigratingASubstreamGrainWithThreeSubstreamsDoesNotFailTheStream()
        {
            var sql = @"
            CREATE TABLE migrate3_left (val any);
            CREATE TABLE migrate3_right (val any);

            INSERT INTO migrate3_out
            SELECT l.val FROM migrate3_left l
            INNER JOIN migrate3_right r ON l.val = r.val;
            ";
            TestTableStore.AddRows("migrate3_left", Enumerable.Range(0, 100).Select(x => (long)x));
            TestTableStore.AddRows("migrate3_right", Enumerable.Range(0, 40).Select(x => (long)x));

            var streamGrain = _fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>("orleans_migrate3");
            await streamGrain.StartStreamAsync(new StartStreamRequest(sql, substreamCount: 3));
            await WaitForResult("migrate3_out", Enumerable.Range(0, 40).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            var substreamGrain = _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(
                SubStreamGrainKey.Create("orleans_migrate3", "substream_1"));
            await MigrateAndWaitForNewActivation(substreamGrain);

            TestTableStore.AddRows("migrate3_right", Enumerable.Range(40, 60).Select(x => (long)x));
            await WaitForResult("migrate3_out", Enumerable.Range(0, 100).Select(x => (long)x).ToList(), TimeSpan.FromSeconds(90));

            await AssertNoFailures(streamGrain);
            await StopStream(streamGrain);
        }

        /// <summary>
        /// Requests a migration and waits until the substream's status is served by a new
        /// activation. The request is re-sent while waiting since a migration whose placement
        /// lands on the same silo can be skipped by the runtime.
        /// </summary>
        private static async Task MigrateAndWaitForNewActivation(ISubStreamGrain substreamGrain)
        {
            var activationBefore = (await substreamGrain.GetStatusAsync()).ActivationId;
            Assert.NotNull(activationBefore);

            var deadline = DateTime.UtcNow.AddSeconds(60);
            string? activationAfter;
            do
            {
                await substreamGrain.MigrateAsync();
                await Task.Delay(500);
                activationAfter = (await substreamGrain.GetStatusAsync()).ActivationId;
            } while (activationAfter == activationBefore && DateTime.UtcNow < deadline);
            Assert.True(activationAfter != activationBefore, "The substream grain activation never migrated");
        }

        /// <summary>
        /// A planned migration is not a failure: no substream may have been unreachable or
        /// have gone through the crash recovery path (fail, roll back, regenerate).
        /// </summary>
        private static async Task AssertNoFailures(IStreamGrain streamGrain)
        {
            var status = await streamGrain.GetStatusAsync();
            foreach (var substream in status.Substreams)
            {
                Assert.True(substream.Error == null, $"Substream {substream.SubstreamName} was unreachable after the migration: {substream.Error}");
                Assert.True(
                    substream.LastFailure == null,
                    $"Substream {substream.SubstreamName} went through the failure path during a planned migration: {substream.LastFailure}");
            }
        }

        private static async Task StopStream(IStreamGrain streamGrain)
        {
            // The coordinated stop can exceed a single grain response timeout under parallel
            // test load; the call is idempotent, so it is retried within the deadline.
            var stopDeadline = DateTime.UtcNow.AddSeconds(90);
            while (true)
            {
                try
                {
                    await streamGrain.StopStreamAsync();
                    return;
                }
                catch (TimeoutException) when (DateTime.UtcNow < stopDeadline)
                {
                }
            }
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
            Assert.Fail($"Sink {sink} did not produce the expected result, expected {expected.Count} rows, got {result?.Count.ToString() ?? "null"} rows");
        }
    }
}
