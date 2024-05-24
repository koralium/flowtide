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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.ExchangeTests
{
    public class ExchangeOperatorTests : OperatorTestBase
    {
        [Fact]
        public async Task TestPullBucketOutput()
        {
            var listeners = Trace.Listeners;
            var relation = new ExchangeRelation()
            {
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = new List<FieldReference>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = 0
                            }
                        }
                    }
                },
                Input = new ReadRelation()
                {
                    BaseSchema = new Substrait.Type.NamedStruct() { Names = new List<string>() { "val" } },
                    NamedTable = new Substrait.Type.NamedTable() { Names = new List<string>() { "table" } }
                },
                Targets = new List<ExchangeTarget>()
                {
                    new StandardOutputExchangeTarget()
                    {
                        PartitionIds = new List<int>(){0},
                    },
                    new PullBucketExchangeTarget()
                    {
                        PartitionIds = new List<int>(){0},
                        ExchangeTargetId = 0
                    }
                },
                PartitionCount = 1
            };
            var op = new ExchangeOperator(relation, FunctionsRegister, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });

            var outputBlock = new BufferBlock<IStreamEvent>();
            op.Sources[0].LinkTo(outputBlock);
            await InitializeOperator(op);

            await op.SendAsync(new InitWatermarksEvent(new HashSet<string>() { "table" }));

            List<RowEvent> events =
            [
                new RowEvent(1, 0, new CompactRowData(FlexBuffers.FlexBufferBuilder.Vector(v =>
                {
                    v.Add("1");
                }))),
                new RowEvent(1, 0, new CompactRowData(FlexBuffers.FlexBufferBuilder.Vector(v =>
                {
                    v.Add("2");
                })))
            ];

            await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(events), 0));

            await outputBlock.OutputAvailableAsync();
            var initWatermarkMessage = await outputBlock.ReceiveAsync();
            var rowsMessage = await outputBlock.ReceiveAsync();

            var fetchData = new ExchangeFetchDataMessage()
            {
                FromEventId = 0
            };
            await op.QueueTrigger(new TriggerEvent("exchange_0", fetchData));

            Assert.NotNull(fetchData.OutEvents);
            // Compare events from outputs
            var fetchDataRowEvents = (fetchData.OutEvents[1] as StreamMessage<StreamEventBatch>)!;
            
            for (int i = 0; i < events.Count; i++)
            {
                Assert.Equal(events[i].ToJson(), fetchDataRowEvents.Data.Events[i].ToJson());
            }
        }

        /// <summary>
        /// This test checks that standard output works when a pull bucket is the first target.
        /// This has to be handled since standardoutputs are connected using their order and not the targetid.
        /// This is to reduce the amount of blocks that are created for standard output.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestStandardOutputAsSecondTarget()
        {
            var listeners = Trace.Listeners;
            var relation = new ExchangeRelation()
            {
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = new List<FieldReference>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = 0
                            }
                        }
                    }
                },
                Input = new ReadRelation()
                {
                    BaseSchema = new Substrait.Type.NamedStruct() { Names = new List<string>() { "val" } },
                    NamedTable = new Substrait.Type.NamedTable() { Names = new List<string>() { "table" } }
                },
                Targets = new List<ExchangeTarget>()
                {
                    new PullBucketExchangeTarget()
                    {
                        PartitionIds = new List<int>(){0},
                        ExchangeTargetId = 0
                    },
                    new StandardOutputExchangeTarget()
                    {
                        PartitionIds = new List<int>(){0},
                    }
                },
                PartitionCount = 1
            };
            var op = new ExchangeOperator(relation, FunctionsRegister, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });

            var outputBlock = new BufferBlock<IStreamEvent>();
            op.Sources[0].LinkTo(outputBlock);
            await InitializeOperator(op);

            await op.SendAsync(new InitWatermarksEvent(new HashSet<string>() { "table" }));

            List<RowEvent> events =
            [
                new RowEvent(1, 0, new CompactRowData(FlexBuffers.FlexBufferBuilder.Vector(v =>
                {
                    v.Add("1");
                }))),
                new RowEvent(1, 0, new CompactRowData(FlexBuffers.FlexBufferBuilder.Vector(v =>
                {
                    v.Add("2");
                })))
            ];

            await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(events), 0));

            await outputBlock.OutputAvailableAsync();
            var initWatermarkMessage = await outputBlock.ReceiveAsync();
            var rowsMessage = await outputBlock.ReceiveAsync();

            var fetchData = new ExchangeFetchDataMessage()
            {
                FromEventId = 0
            };
            await op.QueueTrigger(new TriggerEvent("exchange_0", fetchData));

            Assert.NotNull(fetchData.OutEvents);
            // Compare events from outputs
            var fetchDataRowEvents = (fetchData.OutEvents[1] as StreamMessage<StreamEventBatch>)!;

            for (int i = 0; i < events.Count; i++)
            {
                Assert.Equal(events[i].ToJson(), fetchDataRowEvents.Data.Events[i].ToJson());
            }
        }
    }
}
