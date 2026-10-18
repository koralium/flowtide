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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CleanupStrictHeaderTests
    {
        [Theory]
        [InlineData("\"timestamp\":1,\"timestamp\":2")]
        [InlineData("\"operation\":\"WRITE\",\"operation\":\"WRITE\"")]
        [InlineData("\"flowtide.createdFiles\":[],\"flowtide.createdFiles\":[]")]
        [InlineData("\"flowtide.stageId\":\"stage\"")]
        [InlineData("\"flowtide.adoptedAt\":0")]
        public void CleanupAnchorRejectsEveryDuplicatedFixedField(string extra)
        {
            var bytes = Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.stageId\":\"stage\",\"flowtide.adoptedAt\":0," + extra + "}}\n");
            using var stream = new ProofStream(new MemoryStream(bytes), null, proveFirstLine: true);
            stream.CopyTo(Stream.Null);
            var read = stream.Complete(1);
            var content = new ContentProof("/t/_delta_log/00000000000000000001.json", 1, false,
                new ObjectIdentity("t", 1, bytes.Length, null, null), read.Length, read.Sha256!)
            {
                Provenance = read.Provenance
            };

            Assert.Equal(AnchorCheck.NotOwn, CommitAnchor.TryCreate("table-a", content, "stage", out _));
        }
    }
}
