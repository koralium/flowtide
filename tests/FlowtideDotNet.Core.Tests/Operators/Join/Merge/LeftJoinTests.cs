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

//using FlowtideDotNet.Base.Engine.Internal;
//using FlowtideDotNet.Base;
//using FlowtideDotNet.Core.Operators.Join.MergeJoin;
//using FlowtideDotNet.Storage.StateManager;
//using FASTER.core;
//using FlowtideDotNet.Substrait.Expressions.Literals;
//using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;
//using FlowtideDotNet.Substrait.Expressions;
//using FlowtideDotNet.Substrait.Relations;
//using System.Threading.Tasks.Dataflow;
//using Microsoft.Extensions.Logging.Abstractions;
//using System.Diagnostics.Metrics;

//namespace FlowtideDotNet.Core.Tests.Operators.Join.Merge
//{
//    public class LeftJoinTests
//    {
//        MergeJoinOperatorBase op;
//        BufferBlock<IStreamEvent> recieveBuffer;
//        public LeftJoinTests()
//        {
//            InitTests().GetAwaiter().GetResult();
//        }

//        private async Task InitTests()
//        {
//            op = new MergeJoinOperatorBase(new FlowtideDotNet.Substrait.Relations.MergeJoinRelation()
//            {
//                LeftKeys = new List<FieldReference>()
//                {
//                    new DirectFieldReference()
//                    {
//                        ReferenceSegment = new StructReferenceSegment()
//                        {
//                            Field = 0
//                        }
//                    }
//                },
//                RightKeys = new List<FieldReference>()
//                {
//                    new DirectFieldReference()
//                    {
//                        ReferenceSegment = new StructReferenceSegment()
//                        {
//                            Field = 2
//                        }
//                    }
//                },
//                PostJoinFilter = new BooleanComparison()
//                {
//                    Left = new DirectFieldReference()
//                    {
//                        ReferenceSegment = new StructReferenceSegment()
//                        {
//                            Field = 4
//                        }
//                    },
//                    Right = new StringLiteral()
//                    {
//                        Value = "v1"
//                    },
//                    Type = BooleanComparisonType.Equals
//                },
//                Left = new ReadRelation()
//                {
//                    BaseSchema = new FlowtideDotNet.Substrait.Type.NamedStruct()
//                    {
//                        Names = new List<string>()
//                        {
//                            "c1",
//                            "c2"
//                        }
//                    }
//                },
//                Right = new ReadRelation()
//                {
//                    BaseSchema = new FlowtideDotNet.Substrait.Type.NamedStruct()
//                    {
//                        Names = new List<string>()
//                        {
//                            "c1",
//                            "c2",
//                            "c3"
//                        }
//                    }
//                },
//                Type = JoinType.Left
//            }, new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
//            {
//                BoundedCapacity = 100,
//                MaxDegreeOfParallelism = 1
//            });
//            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
//            localStorage.Initialize("./data/temp");
//            StateManager stateManager = new StateManager<object>(new StateManagerOptions()
//            {
//                CachePageCount = 1000,
//                LogDevice = localStorage.Get(new FileDescriptor("persistent", "perstitent.log")),
//                CheckpointDir = "./data",
//                TemporaryStorageFactory = localStorage
//            });
//            await stateManager.InitializeAsync();
//            var stateClient = stateManager.GetOrCreateClient("node");

//            var vertexHandler = new VertexHandler("mergejoinstream", "op", (time) =>
//            {

//            }, async (v1, v2, time) =>
//            {

//            }, new Meter("test"), stateClient, new NullLoggerFactory());

//            recieveBuffer = new BufferBlock<IStreamEvent>();

//            op.LinkTo(recieveBuffer);
//            op.Setup("mergejoinstream", "op");
//            op.CreateBlock();
//            op.Link();

//            await op.Initialize("test", 0, 0, null, vertexHandler);

//            await op.Targets[0].SendAsync(new InitWatermarksEvent(new HashSet<string>()
//                {
//                    "left"
//                }));

//            await op.Targets[1].SendAsync(new InitWatermarksEvent(new HashSet<string>()
//                {
//                    "right"
//                }));

//            // wait for the init watermark event
//            var initWatermarkEv = await recieveBuffer.ReceiveAsync();
//        }

//        [Fact]
//        public async Task TestLeftJoin()
//        {
//            var leftEvent = StreamEvent.Create(1, 0, b =>
//            {
//                b.Add("c1value");
//            });

//            await op.Targets[0].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
//            {
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("c1value1");
//                }),
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("c1value2");
//                }),
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("c1value3");
//                })
//            }), 0));

//            var response = await recieveBuffer.ReceiveAsync();

//            if (response is StreamMessage<StreamEventBatch> eventBatch)
//            {
//                Assert.Equal(3, eventBatch.Data.Events.Count);
//                Assert.Equal(1, eventBatch.Data.Events[0].Weight);
//                Assert.Equal("[\"c1value1\",null]", eventBatch.Data.Events[0].Vector.ToJson);
//                Assert.Equal(1, eventBatch.Data.Events[1].Weight);
//                Assert.Equal("[\"c1value2\",null]", eventBatch.Data.Events[1].Vector.ToJson);
//                Assert.Equal(1, eventBatch.Data.Events[2].Weight);
//                Assert.Equal("[\"c1value3\",null]", eventBatch.Data.Events[2].Vector.ToJson);
//            }
//            else
//            {
//                Assert.Fail("Expected event batch");
//            }

//            await op.Targets[1].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
//            {
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("c1value2");
//                })
//            }), 0));

//            response = await recieveBuffer.ReceiveAsync();
//            if (response is StreamMessage<StreamEventBatch> eventBatchChange)
//            {
//                Assert.Equal(1, eventBatchChange.Data.Events[0].Weight);
//                Assert.Equal("[\"c1value2\",\"c1value2\"]", eventBatchChange.Data.Events[0].Vector.ToJson);
//                Assert.Equal(-1, eventBatchChange.Data.Events[1].Weight);
//                Assert.Equal("[\"c1value2\",null]", eventBatchChange.Data.Events[1].Vector.ToJson);
//            }
//            else
//            {
//                Assert.Fail("Expected event batch");
//            }
//        }



//        [Fact]
//        public async Task TestRightLeftLeft()
//        {
//            //var leftEvent = StreamEvent.Create(1, b =>
//            //{
//            //    b.Add("c1value");
//            //});

//            await op.Targets[1].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
//            {
//                 StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("1");
//                    b.Add("right1");
//                    b.Add("v2");
//                }),
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("1");
//                    b.Add("right2");
//                    b.Add("v1");
//                })
//            }), 0));


//            await Task.Delay(100);
//            //var response = await recieveBuffer.ReceiveAsync();

//            await op.Targets[0].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
//            {
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("1");
//                    b.Add("left1");
//                }),
//            }), 0));

//            var response = await recieveBuffer.ReceiveAsync();

//            await op.Targets[0].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
//            {
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("1");
//                    b.Add("left2");
//                }),
//                StreamEvent.Create(-1, 0, b =>
//                {
//                    b.Add("1");
//                    b.Add("left1");
//                }),
//            }), 0));

//            response = await recieveBuffer.ReceiveAsync();

//            //await op.Targets[0].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
//            //{
//            //    StreamEvent.Create(-1, b =>
//            //    {
//            //        b.Add("1");
//            //        b.Add("left1");
//            //    }),
//            //}), 0));

//            //response = await recieveBuffer.ReceiveAsync();

//            if (response is StreamMessage<StreamEventBatch> eventBatch)
//            {
//                Assert.Equal(3, eventBatch.Data.Events.Count);
//                Assert.Equal(1, eventBatch.Data.Events[0].Weight);
//                Assert.Equal("[\"c1value1\",null]", eventBatch.Data.Events[0].Vector.ToJson);
//                Assert.Equal(1, eventBatch.Data.Events[1].Weight);
//                Assert.Equal("[\"c1value2\",null]", eventBatch.Data.Events[1].Vector.ToJson);
//                Assert.Equal(1, eventBatch.Data.Events[2].Weight);
//                Assert.Equal("[\"c1value3\",null]", eventBatch.Data.Events[2].Vector.ToJson);
//            }
//            else
//            {
//                Assert.Fail("Expected event batch");
//            }

            

//            response = await recieveBuffer.ReceiveAsync();
//            if (response is StreamMessage<StreamEventBatch> eventBatchChange)
//            {
//                Assert.Equal(1, eventBatchChange.Data.Events[0].Weight);
//                Assert.Equal("[\"c1value2\",\"c1value2\"]", eventBatchChange.Data.Events[0].Vector.ToJson);
//                Assert.Equal(-1, eventBatchChange.Data.Events[1].Weight);
//                Assert.Equal("[\"c1value2\",null]", eventBatchChange.Data.Events[1].Vector.ToJson);
//            }
//            else
//            {
//                Assert.Fail("Expected event batch");
//            }
//        }

//    }
//}
