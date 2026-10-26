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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection("Probes")]
    public class ProbeKitTests
    {
        [Fact]
        public async Task EveryRunStartsFromTheBuiltTable()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            using var fixture = await ProbeFixture.Build("kit_fixture", 3, 100, null);
            var users = fixture.Files.SelectMany(x => x).Take(10).ToList();
            await using (var first = await fixture.Open(null))
            {
                Assert.NotNull(await first.Measure(() => first.Stream.DeleteUsers(users)));
                Assert.NotNull(await first.Measure(() => first.Stream.AddUsers(users)));
            }

            await using var second = await fixture.Open(null);
            var table = await DeltaTransactionReader.ReadTable(second.Storage, fixture.Name);

            Assert.Equal(3, table!.AddFiles.Count);
            Assert.All(table.AddFiles, x => Assert.Null(x.DeletionVector));
            Assert.Equal(fixture.Rows, await second.LiveRows());
        }

        [Fact]
        public async Task MeasureWaitsForPublication()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            const string name = "kit_publication";
            await using var table = await ProbeTable.Start(name, UserInsert, null);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var commit = $"/{name}/_delta_log/00000000000000000001.json";
            table.Storage.DeferPublication = path => path.Full == commit ? release.Task : null;
            table.Storage.PublicationParked = path =>
            {
                if (path.Full == commit)
                {
                    parked.TrySetResult();
                }
            };

            try
            {
                var measured = table.Measure(() => table.Stream.GenerateUsers(1), TimeSpan.FromSeconds(2));
                await parked.Task.WaitAsync(TimeSpan.FromSeconds(30));
                var sample = await measured.WaitAsync(TimeSpan.FromSeconds(30));

                Assert.Null(sample);
            }
            finally
            {
                release.TrySetResult();
            }
        }

        [Fact]
        public async Task FailedMeasureStopsItsSampler()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            await using var table = await ProbeTable.Start("kit_sampler", UserInsert, null);
            var before = PeakWorkingSet.Running;
            table.Storage.Before = (verb, path) => verb == "OpenWrite" && path.Full.EndsWith(".parquet", StringComparison.Ordinal)
                ? Task.FromException(new IOException("Injected write failure"))
                : Task.CompletedTask;

            var failure = await Assert.ThrowsAnyAsync<Exception>(() => table.Measure(() => table.Stream.GenerateUsers(1), TimeSpan.FromSeconds(30)));

            Assert.Contains("Injected write failure", failure.ToString());
            Assert.Equal(before, PeakWorkingSet.Running);
        }

        [Fact]
        public async Task DidNotFinishStopsTheCheckpoint()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            using var fixture = await ProbeFixture.Build("kit_deadline", 5, 100, null);
            // One user per file, so every file is read
            var users = fixture.Files.Select(x => x[0]).ToList();
            await using var table = await fixture.Open(null);
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var reads = 0;
            var inner = table.Storage.Before!;
            table.Storage.Before = async (verb, path) =>
            {
                await inner(verb, path);
                if (verb == "OpenRead" && path.Full.EndsWith(".parquet", StringComparison.Ordinal) && Interlocked.Increment(ref reads) == 1)
                {
                    // The first read waits, so the deadline passes inside the reads
                    await gate.Task;
                }
            };

            int atDeadline;
            try
            {
                // The gate holds the first read past the deadline
                Assert.Null(await table.Measure(() => table.Stream.DeleteUsers(users), TimeSpan.FromSeconds(5)));
                atDeadline = Volatile.Read(ref reads);
            }
            finally
            {
                gate.TrySetResult();
            }
            await table.DisposeAsync();

            Assert.Equal(1, atDeadline);
            Assert.Equal(atDeadline, Volatile.Read(ref reads));
        }
    }
}
