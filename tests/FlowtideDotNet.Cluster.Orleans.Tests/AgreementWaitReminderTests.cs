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

using FlowtideDotNet.Cluster.Orleans.Grains;
using FlowtideDotNet.Cluster.Orleans.Interfaces;
using FlowtideDotNet.Cluster.Orleans.Internal;
using FlowtideDotNet.Cluster.Orleans.Messages;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    public class AgreementWaitReminderTests : IClassFixture<OrleansClusterFixture>
    {
        private const int Bound = 4;
        private readonly OrleansClusterFixture _fixture;

        public AgreementWaitReminderTests(OrleansClusterFixture fixture)
        {
            _fixture = fixture;
            // The fixture is per class, so the small bound only reaches these tests.
            ((InProcessSiloHandle)fixture.Cluster.Primary).SiloHost.Services.GetRequiredService<FlowtideOrleansOptions>().AgreementWaitReminderTicks = Bound;
        }

        // The first activation of substream_0 never receives its peer's claims, only a new activation heals it.
        [Fact]
        public async Task EndlessAgreementWaitIsRecreatedAfterTheBound()
        {
            var (streamGrain, waiter, waiterKey) = Prepare("u4stuck");
            string? starved = null;
            SubStreamGrain.DropDurabilityClaimForTests = (key, activation) =>
                key == waiterKey && (Interlocked.CompareExchange(ref starved, activation, null) ?? activation) == activation;
            try
            {
                var first = await StartAndWaitForTheWait(streamGrain, waiter, "u4stuck");
                for (int tick = 0; tick < Bound; tick++)
                {
                    await Tick(waiter);
                }
                Assert.Equal(first, (await waiter.GetStatusAsync()).ActivationId);

                // Past the bound: recreated, the next tick activates the new activation.
                await Tick(waiter);
                await Tick(waiter);
                await WaitForFullResult(streamGrain, "u4stuck", $"substream_0 activation {first}");
                Assert.NotEqual(first, (await waiter.GetStatusAsync()).ActivationId);
            }
            finally
            {
                await Cleanup(streamGrain);
            }
        }

        // A wait that ends below the bound keeps the activation.
        [Fact]
        public async Task AgreementWaitBelowTheBoundIsNotRecreated()
        {
            var (streamGrain, waiter, waiterKey) = Prepare("u4legit");
            var blocked = true;
            SubStreamGrain.DropDurabilityClaimForTests = (key, _) => key == waiterKey && Volatile.Read(ref blocked);
            try
            {
                var first = await StartAndWaitForTheWait(streamGrain, waiter, "u4legit");
                for (int tick = 0; tick < Bound - 1; tick++)
                {
                    await Tick(waiter);
                }
                Assert.False(IsFullResult("u4legit"), "The group ran while substream_0 was starved of claims.");

                Volatile.Write(ref blocked, false);
                await WaitForFullResult(streamGrain, "u4legit", $"substream_0 activation {first}");
                Assert.Equal(first, (await waiter.GetStatusAsync()).ActivationId);
            }
            finally
            {
                await Cleanup(streamGrain);
            }
        }

        private (IStreamGrain, ISubStreamGrain, string) Prepare(string prefix)
        {
            var waiterKey = SubStreamGrainKey.Create($"orleans_{prefix}", "substream_0");
            TestTableStore.AddRows($"{prefix}_left", Enumerable.Range(0, 20).Select(x => (long)x));
            TestTableStore.AddRows($"{prefix}_right", Enumerable.Range(0, 10).Select(x => (long)x));
            return (_fixture.Cluster.GrainFactory.GetGrain<IStreamGrain>($"orleans_{prefix}"),
                _fixture.Cluster.GrainFactory.GetGrain<ISubStreamGrain>(waiterKey), waiterKey);
        }

        // Starts the join and returns once substream_1 runs while substream_0 still waits for its claims.
        private static async Task<string?> StartAndWaitForTheWait(IStreamGrain streamGrain, ISubStreamGrain waiter, string prefix)
        {
            await streamGrain.StartStreamAsync(new StartStreamRequest($@"
                CREATE TABLE {prefix}_left (val any);
                CREATE TABLE {prefix}_right (val any);
                INSERT INTO {prefix}_out
                SELECT l.val FROM {prefix}_left l INNER JOIN {prefix}_right r ON l.val = r.val;", substreamCount: 2));
            var deadline = DateTime.UtcNow.AddSeconds(60);
            while ((await streamGrain.GetStatusAsync()).Substreams.First(s => s.SubstreamName == "substream_1").State != Base.Engine.StreamStateValue.Running)
            {
                Assert.True(DateTime.UtcNow < deadline, $"substream_1 never started: {await Describe(streamGrain)}");
                await Task.Delay(200);
            }
            await Task.Delay(TimeSpan.FromSeconds(2));
            var status = await waiter.GetStatusAsync();
            Assert.Equal(Base.Engine.StreamStateValue.Starting, status.State);
            Assert.False(IsFullResult(prefix), "The group ran while substream_0 was starved of claims.");
            return status.ActivationId;
        }

        // Keep alive ticks are delivered directly, the registered period is one minute.
        private static async Task Tick(ISubStreamGrain waiter)
        {
            await waiter.AsReference<IRemindable>().ReceiveReminder("flowtide_keepalive", default);
            await Task.Delay(TimeSpan.FromSeconds(1));
        }

        private static bool IsFullResult(string prefix) =>
            TestTableStore.GetResult($"{prefix}_out")?.SequenceEqual(Enumerable.Range(0, 10).Select(x => (long)x)) == true;

        private static async Task WaitForFullResult(IStreamGrain streamGrain, string prefix, string context)
        {
            var deadline = DateTime.UtcNow.AddSeconds(60);
            while (!IsFullResult(prefix))
            {
                if (DateTime.UtcNow >= deadline)
                {
                    var dump = SharedRingBufferLogger.Dump($"agreement_wait_{prefix}_{DateTime.UtcNow:HHmmss}.log");
                    Assert.Fail($"The group never produced the full result ({context} at the start): {await Describe(streamGrain)}, got [{string.Join(",", TestTableStore.GetResult($"{prefix}_out") ?? new List<long>())}]. Log dump: {dump}");
                }
                await Task.Delay(200);
            }
        }

        private static async Task<string> Describe(IStreamGrain streamGrain)
        {
            var status = await streamGrain.GetStatusAsync();
            return string.Join(" | ", status.Substreams.Select(s => $"{s.SubstreamName}={s.State} ({s.ActivationId}) fail={(s.LastFailure == null ? "-" : s.LastFailure.Split('\n')[0])}"));
        }

        private static async Task Cleanup(IStreamGrain streamGrain)
        {
            SubStreamGrain.DropDurabilityClaimForTests = null;
            await Task.WhenAny(streamGrain.StopStreamAsync(), Task.Delay(TimeSpan.FromSeconds(60)));
        }
    }
}
