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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using System.Collections.Concurrent;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("StreamContext test hooks")]
    public class RollbackRequestRefusalTests : FlowtideAcceptanceBase
    {
        public RollbackRequestRefusalTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
        }

        /// <summary>
        /// All streams of a group are within one version of each other, nothing that still matters asks for less.
        /// </summary>
        [Fact]
        public async Task ARequestFarBelowTheDurableVersionIsRefused()
        {
            await RunAndRequest(committed => 0);
        }

        /// <summary>
        /// One below the durable version can be asked for, but not once that version was committed.
        /// </summary>
        [Fact]
        public async Task ARequestBelowACommittedVersionIsRefused()
        {
            await RunAndRequest(committed => committed - 1);
        }

        private async Task RunAndRequest(Func<long, long> requestedVersion)
        {
            var restores = new ConcurrentQueue<long>();
            long committing = -1;
            var streamName = StreamName;
            StreamContext.RestoreVersionForTests = (name, version) =>
            {
                if (name == streamName)
                {
                    restores.Enqueue(version);
                }
            };
            StreamContext.CheckpointCommitHookForTests = (name, lastVersion) =>
            {
                if (name == streamName)
                {
                    Volatile.Write(ref committing, lastVersion + 1);
                }
                return Task.CompletedTask;
            };
            try
            {
                GenerateData(10);
                await StartStream(@"
                INSERT INTO output
                SELECT userkey FROM users
                ");
                await WaitForUpdate();
                for (int i = 0; i < 3; i++)
                {
                    GenerateUsers(10);
                    await WaitForUpdate();
                }
                // The cycle of the last version is complete, its version was passed to CommitVersion.
                await WaitForCheckpointsToSettle();
                var committed = Volatile.Read(ref committing);
                Assert.True(committed >= 3, $"only reached version {committed}");

                await MockIngressFailAndRollback(requestedVersion(committed));

                await Task.Delay(TimeSpan.FromSeconds(2));
                Assert.Empty(restores);
                Assert.Equal(StreamStateValue.Running, State);

                // Nothing happened to the stream, it keeps working.
                GenerateUsers(10);
                await WaitForUpdate();
                AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
            }
            finally
            {
                StreamContext.RestoreVersionForTests = null;
                StreamContext.CheckpointCommitHookForTests = null;
            }
        }
    }
}
