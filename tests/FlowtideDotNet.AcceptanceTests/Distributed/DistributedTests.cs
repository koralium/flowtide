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
            TestSubstreamComFactory comFactory = new TestSubstreamComFactory((v, epoch) =>
            {
                currentVersion = v;
                numberOfcheckpoints++;
                return Task.CompletedTask;
            }, (v) =>
            {
                currentVersion = v;
                return Task.CompletedTask;
            }, (v, epoch) =>
            {
                return Task.FromResult(new SubstreamInitializeResponse(false, true, currentVersion));
            });
            // The version mismatch triggers an intentional fail and recover without an exception
            AllowFailureAndRecover();
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

        /// <summary>
        /// Happy path test where the other substream is on the same version.
        /// The other substream acknowledges each checkpoint done message with its own
        /// checkpoint done, which allows this stream to set dependencies done and
        /// fully complete its checkpoints.
        /// </summary>
        [Fact]
        public async Task TestMatchingVersionCompletesCheckpointWithDependencies()
        {
            int numberOfcheckpoints = 0;
            int numberOfFailAndRecover = 0;
            long capturedSelfEpoch = 0;
            TestSubstreamComFactory comFactory = null!;
            comFactory = new TestSubstreamComFactory(async (v, targetEpoch) =>
            {
                Interlocked.Increment(ref numberOfcheckpoints);
                // Simulate the other substream completing its own checkpoint, acked with this
                // stream's current epoch (learned through the handshake) so it is not fenced.
                await comFactory.ComHandler.CallRecieveCheckpointDone(v, Volatile.Read(ref capturedSelfEpoch));
            }, (v) =>
            {
                Interlocked.Increment(ref numberOfFailAndRecover);
                return Task.CompletedTask;
            }, (v, selfEpoch) =>
            {
                // Record the epoch this stream announced so acks can carry it, and respond with
                // the same restore version as requested, no recovery needed.
                Volatile.Write(ref capturedSelfEpoch, selfEpoch);
                return Task.FromResult(new SubstreamInitializeResponse(false, true, v));
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

            GenerateData();

            await WaitForUpdate();

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
            Assert.Equal(0, numberOfFailAndRecover);
            Assert.True(numberOfcheckpoints >= 2, $"Expected at least two checkpoints, got {numberOfcheckpoints}");
        }

        /// <summary>
        /// A checkpoint-done ack completes a checkpoint's cross-substream dependency, which lets the
        /// checkpoint finalize (compact) and the next checkpoint start. The ack must only count when
        /// it belongs to the stream's current epoch: an ack tagged with a stale epoch (the peer's
        /// aborted-epoch ack delivered late after this stream restarted) must be dropped, otherwise
        /// it completes cycles the peer never acked, breaking the transient-queue invariant. Here the
        /// peer only ever sends stale-epoch acks; a fenced stream drops them, so no checkpoint's
        /// dependency completes and checkpoints stall (like a stream that receives no acks at all,
        /// see TestWrongHigherStartupVersionInOtherSubstream). The buggy version credits the stale
        /// acks and keeps completing checkpoints.
        /// </summary>
        [Fact]
        public async Task StaleEpochCheckpointDoneAcksDoNotCompleteCheckpoints()
        {
            int checkpointsSeen = 0;
            long capturedSelfEpoch = 0;
            long lastSentCheckpointVersion = -1;
            TestSubstreamComFactory comFactory = null!;
            comFactory = new TestSubstreamComFactory(async (v, targetEpoch) =>
            {
                Interlocked.Increment(ref checkpointsSeen);
                Volatile.Write(ref lastSentCheckpointVersion, v);
                // The peer only ever acks with a STALE epoch (one generation older than the current),
                // exactly as an aborted-epoch ack delivered late after a restart would.
                await comFactory.ComHandler.CallRecieveCheckpointDone(v, Volatile.Read(ref capturedSelfEpoch) - 1);
            }, (v) =>
            {
                return Task.CompletedTask;
            }, (restore, selfEpoch) =>
            {
                Volatile.Write(ref capturedSelfEpoch, selfEpoch);
                return Task.FromResult(new SubstreamInitializeResponse(false, true, restore));
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

            // Drive the scheduler and feed data repeatedly. Every checkpoint only ever receives a
            // stale-epoch ack. A fenced stream drops them, so phase two never completes and no new
            // checkpoint can start; the count stays at the one checkpoint that reached its notify
            // phase. The buggy version credits the stale acks and the count climbs with the data.
            for (int i = 0; i < 12; i++)
            {
                GenerateData();
                await SchedulerTick();
                await Task.Delay(100);
            }

            int seen = Volatile.Read(ref checkpointsSeen);

            // Let any stalled checkpoint finish with a current-epoch ack so dispose does not hang.
            long lastVersion = Volatile.Read(ref lastSentCheckpointVersion);
            if (lastVersion >= 0)
            {
                await comFactory.ComHandler.CallRecieveCheckpointDone(lastVersion, Volatile.Read(ref capturedSelfEpoch));
            }

            Assert.True(
                seen <= 2,
                $"Checkpoints kept completing on stale-epoch acks ({seen}); the checkpoint-done ack was not fenced by epoch.");
        }
    }
}
