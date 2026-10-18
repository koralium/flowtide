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
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CommitProvenanceIndependenceTests
    {
        [Fact]
        public void ValidEpochIsInheritedWhenPredecessorTimestampIsInvalid()
        {
            var header = CommitHeaderReader.Read(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.stageId\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"flowtide.adoptedAt\":0,\"timestamp\":\"bad\"}}"), terminated: true, HeaderMode.Lenient);
            Assert.True(header.IsCommitInfo, header.Error);
            Assert.Equal(0, header.AdoptedAt);
            Assert.Null(header.Timestamp);

            var provenance = CommitProvenance.From(5, header);

            Assert.Equal(0, CommitProvenance.NextEpoch(provenance, 6));
            Assert.Equal(50, CommitProvenance.NextTimestamp(50, provenance));
        }

        [Fact]
        public void ValidTimestampAdvancesWhenOptionalLineageIsInvalid()
        {
            var header = CommitHeaderReader.Read(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.stageId\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"flowtide.adoptedAt\":\"bad\",\"timestamp\":100}}"), terminated: true, HeaderMode.Lenient);
            Assert.True(header.IsCommitInfo, header.Error);
            Assert.Null(header.AdoptedAt);
            Assert.Equal(100, header.Timestamp);

            var provenance = CommitProvenance.From(5, header);

            Assert.Equal(6, CommitProvenance.NextEpoch(provenance, 6));
            Assert.Equal(101, CommitProvenance.NextTimestamp(50, provenance));
        }
    }
}
