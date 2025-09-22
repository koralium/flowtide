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

using FlowtideDotNet.Core.Operators.Exchange;
using Google.Protobuf;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    public class DistributedTests : FlowtideAcceptanceBase
    {
        public DistributedTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, false)
        {
        }

        /// <summary>
        /// This test checks that a fail and recover is sent if the substream version does not match the current version.
        /// In this test the other substream is on a later version.
        /// 
        /// The test verifies that the stream recovers and processes data correctly.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestWrongHigherStartupVersionInOtherSubstream()
        {
            long currentVersion = 1;
            int numberOfcheckpoints = 0;
            TestSubstreamComFactory comFactory = new TestSubstreamComFactory((v) =>
            {
                currentVersion = v;
                numberOfcheckpoints++;
                return Task.CompletedTask;
            }, (v) =>
            {
                currentVersion = v;
                return Task.CompletedTask;
            }, (v) =>
            {
                return Task.FromResult(new SubstreamInitializeResponse(false, true, currentVersion));
            });
            GenerateData();
            await StartStream(@"
            SUBSTREAM sub1;

            CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

            SUBSTREAM sub2;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ", distributedOptions: new Core.Engine.DistributedOptions("sub1", default, comFactory));

            await WaitForUpdate();

            var act = GetActualRows();

            var expected = Users.Select(x => {
                byte[] bytes = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(bytes, x.UserKey);
                var hashBytes = XxHash32.Hash(bytes);
                var hashInt = BinaryPrimitives.ReadInt32BigEndian(hashBytes);
                return new
                {
                    hash = hashInt % 2,
                    UserKey = x.UserKey
                };
            }).Where(x => x.hash == 0)
            .Select(x =>
            {
                return new { x.UserKey };
            }).ToList();

            AssertCurrentDataEqual(expected);
            Assert.Equal(1, numberOfcheckpoints);
        }
    }
}
