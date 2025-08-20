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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class RollbackTests : FlowtideAcceptanceBase
    {
        public RollbackTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
        }

        /// <summary>
        /// This test will not be able to complete the last wait for update if rolling back does not work.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task RollbackTwoVersionsFromIngress()
        {
            GenerateData(10);

            await StartStream(@"
            INSERT INTO output
            SELECT userkey FROM users
            ");

            // Do a normal checkpoint first
            await WaitForUpdate();

            // Stop dependencies auto complete to not allow any compaction of checkpoints
            // to happen after checkpoint
            await StopMockIngressAutocompleteDependencies();

            GenerateUsers(10);

            await WaitForUpdate();

            // Rollback to the version before these ten events have been read.
            await MockIngressFailAndRollback(1);

            // This will not complete if rolling back two versions do not work
            await WaitForUpdate();

            // Set dependencies done to allow checkpoints to continue
            await MockIngressSetDependenciesDone();

            GenerateUsers(10);

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        /// <summary>
        /// This test makes sure that it is possible to return back to an empty state
        /// if the stream fails before the first checkpoint
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task RollbackToEmptyVersion()
        {

            await StartStream(@"
            INSERT INTO output
            SELECT userkey FROM users
            ");

            // Stop dependencies auto complete to not allow any compaction of checkpoints
            // to happen after checkpoint
            await StopMockIngressAutocompleteDependencies();

            GenerateData(10);

            // Do a normal checkpoint first
            await WaitForUpdate();

            await MockIngressFailAndRollback(0);

            // This will not complete if it was not possible to rollback
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }
    }
}
