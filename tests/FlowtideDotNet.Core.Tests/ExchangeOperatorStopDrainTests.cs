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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// The stop drain of an exchange target: the stream may only finish stopping once the
    /// other substream durably consumed the stop barrier, meaning it committed a checkpoint
    /// covering it. A fetch alone is not delivery (the response can be lost after the
    /// destructive dequeue), and an acknowledgement for a checkpoint the peer committed
    /// BEFORE consuming the barrier proves nothing about the barrier - confirming the drain
    /// on it and disposing the queue loses the in-flight events if the peer then rolls back.
    /// </summary>
    public class ExchangeOperatorStopDrainTests : OperatorTestBase
    {
        private sealed class RecordingHandler : ISubstreamCommunicationHandler
        {
            private Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>>? _getDataFunc;
            private Func<long, long, bool, Task<SubstreamInitializeResponse>>? _initializeFromTarget;
            private Func<long, long, bool, Task>? _callRecieveCheckpointDone;

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _getDataFunc = getDataFunction;
                _initializeFromTarget = initializeFromTarget;
                _callRecieveCheckpointDone = callRecieveCheckpointDone;
            }

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
                => Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier) => Task.CompletedTask;

            public Task SendFailAndRecover(long restoreVersion) => Task.CompletedTask;

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
                => Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));

            /// <summary>
            /// Fetches events from the operator's targets like the peer's fetch loop does.
            /// </summary>
            public async Task<IReadOnlyList<SubstreamEventData>> CallGetData(IReadOnlySet<int> targetIds, int numberOfEvents)
            {
                if (_getDataFunc == null)
                {
                    throw new InvalidOperationException("The handler was not initialized by a communication point.");
                }
                var events = await _getDataFunc(targetIds, numberOfEvents, default);
                foreach (var ev in events)
                {
                    // The test only observes the fetch happening, the receiver claim on the
                    // dequeued events is returned like a delivery failure would.
                    SubstreamCommunicationPoint.DisposeEvent(ev.StreamEvent);
                }
                return events;
            }

            /// <summary>
            /// Delivers a checkpoint done acknowledgement from the peer through the real
            /// communication point, tagged with the epoch the point announced in the
            /// handshake response so the acknowledgement passes the epoch fence.
            /// </summary>
            public async Task DeliverCheckpointDone(long checkpointVersion, bool coversPeerStopBarrier)
            {
                if (_initializeFromTarget == null || _callRecieveCheckpointDone == null)
                {
                    throw new InvalidOperationException("The handler was not initialized by a communication point.");
                }
                var response = await _initializeFromTarget(0, 0, false);
                await _callRecieveCheckpointDone(checkpointVersion, response.CheckpointEpoch, coversPeerStopBarrier);
            }
        }

        private sealed class RecordingHandlerFactory : ISubstreamCommunicationHandlerFactory
        {
            public RecordingHandler Handler { get; } = new RecordingHandler();

            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName) => Handler;
        }

        private static ExchangeOperator CreateOperatorWithOnePeer(RecordingHandlerFactory handlerFactory)
        {
            var relation = new ExchangeRelation()
            {
                PartitionCount = 1,
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
                    new SubstreamExchangeTarget() { ExchangeTargetId = 1, SubstreamName = "peer", PartitionIds = new List<int>() { 0 } }
                }
            };

            var functionsRegister = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functionsRegister);
            var factory = new SubstreamCommunicationPointFactory(
                selfSubstreamName: "self",
                communicationHandlerFactory: handlerFactory);

            return new ExchangeOperator(relation, factory, functionsRegister, new ExecutionDataflowBlockOptions());
        }

        private static async Task WaitUntilAsync(Func<bool> condition, string failure)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (!condition())
            {
                Assert.True(DateTime.UtcNow < deadline, failure);
                await Task.Delay(10);
            }
        }

        [Fact]
        public async Task AckThatDoesNotCoverTheStopBarrierDoesNotConfirmTheDrain()
        {
            var handlerFactory = new RecordingHandlerFactory();
            var op = CreateOperatorWithOnePeer(handlerFactory);
            await InitializeOperator(op);

            // No stop barrier yet, nothing gates the stop.
            Assert.True(op.ReadyToStop);

            // The stop cycle stores the stop barrier into the target queue.
            await ((ITargetBlock<IStreamEvent>)op).SendAsync(new StopStreamCheckpoint(1, 2));
            await WaitUntilAsync(() => !op.ReadyToStop, "The stop barrier was never stored in the exchange target");

            // The peer fetches the barrier; the fetch alone must not finish the stop, the
            // response can still be lost after the destructive dequeue.
            var fetched = await handlerFactory.Handler.CallGetData(new HashSet<int>() { 1 }, 100);
            Assert.Contains(fetched, e => e.StreamEvent is StopStreamCheckpoint);
            Assert.False(op.ReadyToStop);

            // The peer acknowledges a checkpoint it committed BEFORE consuming the barrier
            // (the ack raced in after the fetch). It proves nothing about the barrier, the
            // drain must stay unconfirmed.
            await handlerFactory.Handler.DeliverCheckpointDone(1, coversPeerStopBarrier: false);
            // The acknowledgement fans out on the thread pool, give it time to land.
            await Task.Delay(200);
            Assert.False(op.ReadyToStop,
                "An acknowledgement that does not cover the stop barrier confirmed the stop drain; " +
                "the queue would be disposed while the peer can still roll back to before the barrier.");

            // An acknowledgement stamped as covering the barrier consumption confirms the drain.
            await handlerFactory.Handler.DeliverCheckpointDone(2, coversPeerStopBarrier: true);
            await WaitUntilAsync(() => op.ReadyToStop, "A covering acknowledgement did not confirm the stop drain");
        }
    }
}
