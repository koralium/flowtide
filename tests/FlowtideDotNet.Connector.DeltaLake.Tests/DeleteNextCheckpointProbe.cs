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
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Gate B2c's next checkpoint, also runs unchanged on main
    [Collection("Probes")]
    public class DeleteNextCheckpointProbe
    {
        private readonly ITestOutputHelper _output;

        public DeleteNextCheckpointProbe(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task B2cNextCheckpoint()
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, "B2c next checkpoint, 20 files of 10k rows: inserts and a 20 % delete in one commit, then a one-row delete");
            using var fixture = await ProbeFixture.Build("b2c_next", 20, 10_000, kit);
            var next = new List<ProbeSample>();
            var added = 0;
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                await using var table = await fixture.Open(null);
                var random = new Random(run);
                var rewritten = fixture.Files[0].OrderBy(_ => random.Next()).Take(2_000).ToList();
                var change = await table.Measure(() =>
                {
                    table.Stream.DeleteUsers(rewritten);
                    table.Stream.GenerateUsers(1_000);
                }) ?? throw new TimeoutException("The rewrite commit did not finish");
                var head = (await DeltaTransactionReader.ReadTable(table.Storage, fixture.Name))!.Version;
                added = (await DeltaTransactionReader.ReadVersionCommit(table.Storage, fixture.Name, head))!.AddedFiles.Count;
                await table.Settle();
                var sample = await table.Measure(() => table.Stream.DeleteUsers(new[] { fixture.Files[5][run] }));
                if (sample == null)
                {
                    kit.Write("the one-row delete did not finish");
                    return;
                }
                if (run > 0)
                {
                    next.Add(sample.Value);
                }
            }
            kit.Report($"one-row delete after a commit adding {added} files", next);
        }

    }
}
