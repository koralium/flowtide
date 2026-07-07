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
                Func<long, long, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, Task> callRecieveCheckpointDone)
            {
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch)
            {
                return Task.CompletedTask;
            }

            public Task SendFailAndRecover(long restoreVersion)
            {
                return Task.CompletedTask;
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, CancellationToken cancellationToken)
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

            op.TargetCallDependenciesDone();
            op.TargetCallDependenciesDone();
            op.TargetCallDependenciesDone();

            Assert.Equal(1, op.PendingDependenciesDoneSignalsForTests);
        }
    }
}
