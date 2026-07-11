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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Regression tests for the buffered checkpoint-done acknowledgements in the exchange
    /// operator. Each checkpoint cycle consumes exactly one acknowledgement per peer
    /// substream; buffering more than one per peer lets a later cycle complete without a
    /// real acknowledgement, which breaks the compact-only-after-ack protocol.
    /// </summary>
    public class ExchangeOperatorAckBufferTests : OperatorTestBase
    {
        private class FakeHandler : ISubstreamCommunicationHandler
        {
            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                return Task.CompletedTask;
            }

            public Task SendFailAndRecover(long restoreVersion)
            {
                return Task.CompletedTask;
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
            {
                return Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
            }
        }

        private class FakeHandlerFactory : ISubstreamCommunicationHandlerFactory
        {
            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
            {
                return new FakeHandler();
            }
        }

        /// <summary>
        /// Records the callbacks the communication point wires so a test can deliver
        /// checkpoint done acknowledgements FROM a specific peer through the real point,
        /// which is what gives an acknowledgement its peer identity in production.
        /// </summary>
        private class RecordingHandler : ISubstreamCommunicationHandler
        {
            private Func<long, long, bool, Task<SubstreamInitializeResponse>>? _initializeFromTarget;
            private Func<long, long, bool, Task>? _callRecieveCheckpointDone;

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _initializeFromTarget = initializeFromTarget;
                _callRecieveCheckpointDone = callRecieveCheckpointDone;
            }

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier) => Task.CompletedTask;

            public Task SendFailAndRecover(long restoreVersion) => Task.CompletedTask;

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
            {
                return Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
            }

            /// <summary>
            /// Delivers a checkpoint done acknowledgement from this peer through the real
            /// communication point, tagged with the epoch the point announces in the
            /// handshake response so the acknowledgement passes the epoch fence.
            /// </summary>
            public async Task DeliverCheckpointDone(long checkpointVersion)
            {
                if (_initializeFromTarget == null || _callRecieveCheckpointDone == null)
                {
                    throw new InvalidOperationException("The handler was not initialized by a communication point.");
                }
                var response = await _initializeFromTarget(0, 0, false);
                await _callRecieveCheckpointDone(checkpointVersion, response.CheckpointEpoch, false);
            }
        }

        private class RecordingHandlerFactory : ISubstreamCommunicationHandlerFactory
        {
            public Dictionary<string, RecordingHandler> Handlers { get; } = new Dictionary<string, RecordingHandler>();

            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
            {
                if (!Handlers.TryGetValue(targetSubstreamName, out var handler))
                {
                    handler = new RecordingHandler();
                    Handlers.Add(targetSubstreamName, handler);
                }
                return handler;
            }
        }

        private static ExchangeOperator CreateOperatorWithOnePeer()
        {
            var relation = new ExchangeRelation()
            {
                PartitionCount = 2,
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = new List<FieldReference>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                        }
                    }
                },
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "table1" } },
                    BaseSchema = new NamedStruct()
                    {
                        Names = new List<string>() { "val" },
                        Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                    }
                },
                Targets = new List<ExchangeTarget>()
                {
                    new StandardOutputExchangeTarget() { PartitionIds = new List<int>() { 0 } },
                    new SubstreamExchangeTarget() { ExchangeTargetId = 1, SubstreamName = "peer", PartitionIds = new List<int>() { 1 } }
                }
            };

            var functionsRegister = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functionsRegister);
            var factory = new SubstreamCommunicationPointFactory(
                selfSubstreamName: "self",
                communicationHandlerFactory: new FakeHandlerFactory());

            return new ExchangeOperator(relation, factory, functionsRegister, new ExecutionDataflowBlockOptions());
        }

        /// <summary>
        /// A peer can only have one un-acknowledged cycle in flight, but a stopping peer
        /// runs stop checkpoint cycles that each send an acknowledgement, and they can all
        /// arrive before this streams callback is wired. Buffering more than one per peer
        /// would make a later local cycle complete its dependencies without a real
        /// acknowledgement.
        /// </summary>
        [Fact]
        public async Task BufferedAckSignalsAreCappedAtOnePerPeer()
        {
            var op = CreateOperatorWithOnePeer();
            // Initialized but with the checkpoint done callbacks UNWIRED, the state a
            // starting stream is in when acknowledgements from a running peer arrive.
            await InitializeOperatorWithoutWiring(op);

            op.TargetCallDependenciesDone(1);
            op.TargetCallDependenciesDone(1);
            op.TargetCallDependenciesDone(1);

            Assert.Equal(1, op.PendingDependenciesDoneSignalsForTests);
        }

        private static ExchangeOperator CreateOperatorWithTwoPeers(RecordingHandlerFactory handlerFactory)
        {
            var relation = new ExchangeRelation()
            {
                PartitionCount = 3,
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = new List<FieldReference>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                        }
                    }
                },
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "table1" } },
                    BaseSchema = new NamedStruct()
                    {
                        Names = new List<string>() { "val" },
                        Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                    }
                },
                Targets = new List<ExchangeTarget>()
                {
                    new StandardOutputExchangeTarget() { PartitionIds = new List<int>() { 0 } },
                    new SubstreamExchangeTarget() { ExchangeTargetId = 1, SubstreamName = "peerB", PartitionIds = new List<int>() { 1 } },
                    new SubstreamExchangeTarget() { ExchangeTargetId = 2, SubstreamName = "peerC", PartitionIds = new List<int>() { 2 } }
                }
            };

            var functionsRegister = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functionsRegister);
            var factory = new SubstreamCommunicationPointFactory(
                selfSubstreamName: "self",
                communicationHandlerFactory: handlerFactory);

            return new ExchangeOperator(relation, factory, functionsRegister, new ExecutionDataflowBlockOptions());
        }

        /// <summary>
        /// A checkpoint cycle's cross-substream dependency completes when EVERY peer
        /// substream acknowledged, one credit per peer. Acknowledgements lose their version
        /// on the way in and a stopping peer legitimately sends several (one per stop drain
        /// cycle), so counting them fungibly lets two acknowledgements from one peer stand
        /// in for a peer that never acknowledged - the checkpoint completes and compacts
        /// state the silent peer still needs for a common rollback.
        /// </summary>
        [Fact]
        public async Task AcksFromOnePeerDoNotCompleteAnotherPeersDependency()
        {
            var handlerFactory = new RecordingHandlerFactory();
            var op = CreateOperatorWithTwoPeers(handlerFactory);
            await InitializeOperatorWithoutWiring(op);

            int fired = 0;
            ((IStreamEgressVertex)op).SetCheckpointDoneFunction(
                (name, lockingEvent) => { },
                (name, lockingEvent) => Interlocked.Increment(ref fired));

            var peerB = handlerFactory.Handlers["peerB"];
            var peerC = handlerFactory.Handlers["peerC"];

            // Peer B runs its own stop drain cycles and acknowledges twice with increasing
            // versions while peer C stays silent. The cycle must not complete on those.
            await peerB.DeliverCheckpointDone(1);
            await peerB.DeliverCheckpointDone(2);
            Assert.Equal(0, Volatile.Read(ref fired));

            // Only when the other peer acknowledges is every dependency really done.
            await peerC.DeliverCheckpointDone(1);
            Assert.Equal(1, Volatile.Read(ref fired));

            // B's second acknowledgement was a real one (its own stop cycle) and must carry
            // over to the next cycle instead of being silently dropped: with C's next
            // acknowledgement the following cycle completes without another one from B.
            await peerC.DeliverCheckpointDone(2);
            Assert.Equal(2, Volatile.Read(ref fired));
        }

        /// <summary>
        /// The pre-start buffer must be capped at one signal PER PEER - its stated intent -
        /// not at the number of peers in total: with two peers, two acknowledgements from
        /// one stop-cycling peer would otherwise both buffer and later replay as if both
        /// peers had acknowledged, completing the first checkpoint's dependencies without
        /// the second peer ever storing the barrier.
        /// </summary>
        [Fact]
        public async Task BufferedAckSignalsFromOnePeerDoNotFillAnotherPeersSlot()
        {
            var handlerFactory = new RecordingHandlerFactory();
            var op = CreateOperatorWithTwoPeers(handlerFactory);
            // Initialized but with the checkpoint done callbacks UNWIRED, the state a
            // starting stream is in when acknowledgements from running peers arrive.
            await InitializeOperatorWithoutWiring(op);

            var peerB = handlerFactory.Handlers["peerB"];
            var peerC = handlerFactory.Handlers["peerC"];

            await peerB.DeliverCheckpointDone(1);
            await peerB.DeliverCheckpointDone(2);
            Assert.Equal(1, op.PendingDependenciesDoneSignalsForTests);

            // A signal from the other peer is a distinct credit and must still buffer.
            await peerC.DeliverCheckpointDone(1);
            Assert.Equal(2, op.PendingDependenciesDoneSignalsForTests);
        }
    }
}
