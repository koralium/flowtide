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

using FASTER.core;
using FlexBuffers;
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using System.Collections.Immutable;

namespace FlowtideDotNet.Core.Tests.Operators.Aggregate
{
    public class AggregateTests
    {
        [Fact]
        public async Task TestCase()
        {
            var plan = new AggregateRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Substrait.Type.NamedStruct()
                    {
                        Names = new List<string>() { "1", "2" }
                    },
                    NamedTable = new Substrait.Type.NamedTable()
                    {
                        Names = new List<string>() { "left" }
                    }
                },
                Groupings = new List<AggregateGrouping>
                {
                    new AggregateGrouping()
                    {
                        GroupingExpressions = new List<Expression>()
                        {
                            new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = 0
                                }
                            }
                        }
                    }
                },
                Measures = new List<AggregateMeasure>()
                {
                    new AggregateMeasure()
                    {
                        Measure = new AggregateFunction()
                        {
                            ExtensionUri = FunctionsList.Uri,
                            ExtensionName = FunctionsList.ListAgg,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 1
                                    }
                                }
                            }
                        }
                    }
                }
            };
            var funcReg = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(funcReg);
            var op = new ColumnAggregateOperator(plan, funcReg, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 1000
            });

            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000,
                LogDevice = localStorage.Get(new FileDescriptor("persistent", "perstitent.log")),
                CheckpointDir = "./data",
                TemporaryStorageFactory = localStorage
            }, NullLogger.Instance, new System.Diagnostics.Metrics.Meter(""), "stream");
            await stateManager.InitializeAsync();
            var stateClient = stateManager.GetOrCreateClient("node");

            var recieveBuffer = new BufferBlock<IStreamEvent>(new DataflowBlockOptions()
            {
                BoundedCapacity = -1
            });

            var inputBuffer = new BufferBlock<IStreamEvent>();
            

            inputBuffer.LinkTo(op);
            op.LinkTo(recieveBuffer);
            op.Setup("mergejoinstream", "op");
            op.CreateBlock();
            op.Link();

            var streamMetrics = new StreamMetrics("stream");
            var vertexHandler = new VertexHandler("mergejoinstream", "op", (time) =>
            {

            }, async (v1, v2, time) =>
            {

            }, streamMetrics.GetOrCreateVertexMeter("1", () => "name"), stateClient, new NullLoggerFactory());

            await op.Initialize("test", 0, 0, null, vertexHandler);

            await inputBuffer.SendAsync(new InitWatermarksEvent(new HashSet<string>()
                {
                    "left"
                }));

            //await rightBuffer.SendAsync(new InitWatermarksEvent(new HashSet<string>()
            //{
            //    "right"
            //}));

            var initWatermarkEv = await recieveBuffer.ReceiveAsync();

            var lines = File.ReadAllLines("./Operators/Aggregate/DatetimeFunctionTests_GetTimestampInAggregate_3.all.txt");

            List<RowEvent> rowEvents = new List<RowEvent>();
            for (int i = 0; i < lines.Length; i++)
            {
                var line = lines[i];

                if (line == "Checkpoint")
                {
                    continue;
                }
                if (line == "Restart")
                {
                    continue;
                }
                if (line == "Watermark")
                {
                    if (rowEvents.Count == 0)
                    {
                        continue;
                    }
                    if (op.Completion.IsCompleted || op.Completion.IsFaulted || op.Completion.IsCanceled)
                    {
                        throw op.Completion.Exception;
                        break;
                    }
                    await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(rowEvents, 2), 0));
                    await op.SendAsync(new Watermark("asd", 0));
                    rowEvents = new List<RowEvent>();
                    continue;
                }
                var ind = line.IndexOf('[');
                var weight = int.Parse(line.Substring(0, ind - 1));
                var data = line.Substring(ind, line.Length - ind);
                data = data.Replace("\\", "\\\\");
                if (data.Contains("\\"))
                {

                }
                try
                {
                    var flexBuffer = JsonToFlexBufferConverter.Convert(data);
                    var rowEvent = new RowEvent(weight, 0, new CompactRowData(flexBuffer));
                    rowEvents.Add(rowEvent);
                }
                catch (Exception e)
                {
                    throw;
                }

            }

            //await op.Targets[0].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new List<RowEvent>()
            //{
            //    RowEvent.Create(1, 0, b =>
            //    {
            //        b.Add(1);
            //        b.Add("100");
            //        b.Add("9000001000");
            //        b.AddNull();
            //    })
            //}), 0));

            //await op.Targets[1].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new List<RowEvent>()
            //{
            //    RowEvent.Create(1, 0, b =>
            //    {
            //        b.Add(1);
            //        b.Add("100");
            //    })
            //}), 0));

            var response = await recieveBuffer.ReceiveAsync();

            while (true)
            {

            }
        }
    }
}
