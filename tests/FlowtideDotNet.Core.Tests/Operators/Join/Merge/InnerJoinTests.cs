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
//using FlowtideDotNet.Substrait.Expressions.Literals;
//using FlowtideDotNet.Substrait.Expressions;
//using FlowtideDotNet.Substrait.Relations;
//using System.Threading.Tasks.Dataflow;
//using FlowtideDotNet.Storage.StateManager;
//using FASTER.core;
//using Microsoft.Extensions.Logging.Abstractions;

//namespace FlowtideDotNet.Core.Tests.Operators.Join.Merge
//{
//    public class InnerJoinTests
//    {
//        MergeJoinOperatorBase op;
//        BufferBlock<IStreamEvent> recieveBuffer;
//        public InnerJoinTests()
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
//                            Field = 1
//                        }
//                    }
//                },
//                PostJoinFilter = new BooleanComparison()
//                {
//                    Left = new DirectFieldReference()
//                    {
//                        ReferenceSegment = new StructReferenceSegment()
//                        {
//                            Field = 0
//                        }
//                    },
//                    Right = new StringLiteral()
//                    {
//                        Value = "c1value2"
//                    },
//                    Type = BooleanComparisonType.Equals
//                },
//                Left = new ReadRelation()
//                {
//                    BaseSchema = new FlowtideDotNet.Substrait.Type.NamedStruct()
//                    {
//                        Names = new List<string>()
//                        {
//                            "c1"
//                        }
//                    }
//                },
//                Right = new ReadRelation()
//                {
//                    BaseSchema = new FlowtideDotNet.Substrait.Type.NamedStruct()
//                    {
//                        Names = new List<string>()
//                        {
//                            "c2"
//                        }
//                    }
//                }
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

//            }, null, stateClient, new NullLoggerFactory());

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
//        public async Task TestInnerJoin()
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
//            await op.Targets[1].SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
//            {
//                StreamEvent.Create(1, 0, b =>
//                {
//                    b.Add("c1value2");
//                })
//            }), 0));
//            await op.Targets[0].SendAsync(new Watermark("left", 1));


//            var response = await recieveBuffer.ReceiveAsync();

//            if (response is StreamMessage<StreamEventBatch> eventBatch)
//            {
//                Assert.Equal("[\"c1value2\",\"c1value2\"]", eventBatch.Data.Events.First().Vector.ToJson);
//            }
//            else
//            {
//                Assert.Fail("Expected event batch");
//            }
//        }
//    }
//}
