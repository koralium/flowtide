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
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.StateManager;
using FASTER.core;
using Microsoft.Extensions.Logging.Abstractions;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Storage.Persistence.CacheStorage;

namespace FlowtideDotNet.Core.Tests.Operators.Normalization
{
    // TODO: Enable these tests again
    //public class NormalizationOperatorTests : IDisposable
    //{
    //    NormalizationOperator op;
    //    BufferBlock<IStreamEvent> recieveBuffer;
    //    StateManagerSync stateManager;
    //    VertexHandler vertexHandler;

    //    public NormalizationOperatorTests()
    //    {
    //        InitTests().GetAwaiter().GetResult();
    //    }

    //    private async Task InitTests()
    //    {
    //        op = new NormalizationOperator(new FlowtideDotNet.Substrait.Relations.NormalizationRelation()
    //        {
    //            Input = new ReadRelation()
    //            {
    //                BaseSchema = new FlowtideDotNet.Substrait.Type.NamedStruct()
    //                {
    //                    Names = new List<string>()
    //                    {
    //                        "c1",
    //                        "c2"
    //                    }
    //                }
    //            },
    //            KeyIndex = new List<int>() { 0 }
    //        }, new Compute.FunctionsRegister(), new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
    //        {
    //            BoundedCapacity = 100,
    //            MaxDegreeOfParallelism = 1
    //        });

    //        var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
    //        localStorage.Initialize("./data/temp");
    //        stateManager = new StateManagerSync<object>(() => new StateManagerOptions()
    //        {
    //            CachePageCount = 1000,
    //            PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions())
    //            //LogDevice = Devices.CreateLogDevice("./data/persistent", recoverDevice: true),
    //            //CheckpointDir = "./data",
    //            //TemporaryStorageFactory = localStorage
    //        }, new NullLogger<StateManagerSync<object>>());
    //        await stateManager.InitializeAsync();
    //        var stateClient = stateManager.GetOrCreateClient("node");

    //        StreamMetrics streamMetrics = new StreamMetrics("stream");
    //        var nodeMetrics =  streamMetrics.GetOrCreateVertexMeter("node1");

    //        vertexHandler = new VertexHandler("mergejoinstream", "op", (time) =>
    //        {

    //        }, async (v1, v2, time) =>
    //        {

    //        }, nodeMetrics, stateClient, new NullLoggerFactory());

    //        recieveBuffer = new BufferBlock<IStreamEvent>();

    //        op.LinkTo(recieveBuffer);
    //        op.Setup("mergejoinstream", "op");
    //        op.CreateBlock();
    //        op.Link();

    //        await op.Initialize("test", 0, 0, null, vertexHandler);
    //    }

    //    [Fact]
    //    public async Task Insert()
    //    {
    //        await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<RowEvent>()
    //        {
    //            RowEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value1");
    //                b.Add("c2value1");
    //            }),
    //            RowEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value2");
    //                b.Add("c2value2");
    //            }),
    //            RowEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value3");
    //                b.Add("c2value3");
    //            })
    //        }), 0));

    //        var msg1 = await recieveBuffer.ReceiveAsync();
            
    //        if (msg1 is StreamMessage<StreamEventBatch> msg)
    //        {
    //            Assert.Equal(1, msg.Data.Events[0].Weight);
    //            Assert.Equal("[\"c1value1\",\"c2value1\"]", msg.Data.Events[0]..ToJson);
    //            Assert.Equal(1, msg.Data.Events[1].Weight);
    //            Assert.Equal("[\"c1value2\",\"c2value2\"]", msg.Data.Events[1].Vector.ToJson);
    //            Assert.Equal(1, msg.Data.Events[2].Weight);
    //            Assert.Equal("[\"c1value3\",\"c2value3\"]", msg.Data.Events[2].Vector.ToJson);
    //        }
    //        else
    //        {
    //            Assert.Fail("Wrong return type");
    //        }
            
    //    }

    //    [Fact]
    //    public async Task InsertDelete()
    //    {
    //        await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
    //        {
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value1");
    //                b.Add("c2value1");
    //            }),
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value2");
    //                b.Add("c2value2");
    //            }),
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value3");
    //                b.Add("c2value3");
    //            })
    //        }), 0));

    //        await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
    //        {
    //            StreamEvent.Create(-1, 0, b =>
    //            {
    //                b.Add("c1value1");
    //                b.Add("c2value1");
    //            }),
    //            StreamEvent.Create(-1, 0, b =>
    //            {
    //                b.Add("c1value2");
    //                b.Add("c2value2");
    //            }),
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value3");
    //                b.Add("c2value3");
    //            })
    //        }), 0));

    //        var msg1 = await recieveBuffer.ReceiveAsync();

    //        if (msg1 is StreamMessage<StreamEventBatch> msg)
    //        {
    //            Assert.Equal(1, msg.Data.Events[0].Weight);
    //            Assert.Equal("[\"c1value1\",\"c2value1\"]", msg.Data.Events[0].Vector.ToJson);
    //            Assert.Equal(1, msg.Data.Events[1].Weight);
    //            Assert.Equal("[\"c1value2\",\"c2value2\"]", msg.Data.Events[1].Vector.ToJson);
    //            Assert.Equal(1, msg.Data.Events[2].Weight);
    //            Assert.Equal("[\"c1value3\",\"c2value3\"]", msg.Data.Events[2].Vector.ToJson);
    //        }
    //        else
    //        {
    //            Assert.Fail("Wrong return type");
    //        }
    //        var msg2 = await recieveBuffer.ReceiveAsync();

    //        if (msg2 is StreamMessage<StreamEventBatch> changes)
    //        {
    //            Assert.Equal(2, changes.Data.Events.Count);
    //            Assert.Equal(-1, changes.Data.Events[0].Weight);
    //            Assert.Equal("[\"c1value1\",\"c2value1\"]", changes.Data.Events[0].Vector.ToJson);
    //            Assert.Equal(-1, changes.Data.Events[1].Weight);
    //            Assert.Equal("[\"c1value2\",\"c2value2\"]", changes.Data.Events[1].Vector.ToJson);
    //        }
    //        else
    //        {
    //            Assert.Fail("Wrong return type");
    //        }
    //    }

    //    [Fact]
    //    public async Task InsertUpdate()
    //    {
    //        await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
    //        {
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value1");
    //                b.Add("c2value1");
    //            }),
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value2");
    //                b.Add("c2value2");
    //            }),
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value3");
    //                b.Add("c2value3");
    //            })
    //        }), 0));

    //        await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
    //        {
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value2");
    //                b.Add("c2value2_modify");
    //            })
    //        }), 0));

    //        var msg1 = await recieveBuffer.ReceiveAsync();

    //        if (msg1 is StreamMessage<StreamEventBatch> msg)
    //        {
    //            Assert.Equal(1, msg.Data.Events[0].Weight);
    //            Assert.Equal("[\"c1value1\",\"c2value1\"]", msg.Data.Events[0].Vector.ToJson);
    //            Assert.Equal(1, msg.Data.Events[1].Weight);
    //            Assert.Equal("[\"c1value2\",\"c2value2\"]", msg.Data.Events[1].Vector.ToJson);
    //            Assert.Equal(1, msg.Data.Events[2].Weight);
    //            Assert.Equal("[\"c1value3\",\"c2value3\"]", msg.Data.Events[2].Vector.ToJson);
    //        }
    //        else
    //        {
    //            Assert.Fail("Wrong return type");
    //        }

    //        var msg2 = await recieveBuffer.ReceiveAsync();

    //        if (msg2 is StreamMessage<StreamEventBatch> changes)
    //        {
    //            Assert.Equal(2, changes.Data.Events.Count);
    //            Assert.Equal(1, changes.Data.Events[0].Weight);
    //            Assert.Equal("[\"c1value2\",\"c2value2_modify\"]", changes.Data.Events[0].Vector.ToJson);
    //            Assert.Equal(-1, changes.Data.Events[1].Weight);
    //            Assert.Equal("[\"c1value2\",\"c2value2\"]", changes.Data.Events[1].Vector.ToJson);
    //        }
    //        else
    //        {
    //            Assert.Fail("Wrong return type");
    //        }
    //    }

    //    [Fact]
    //    public async Task FirstCheckpointFailedRestoresEmptyState()
    //    {
    //        await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
    //        {
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value1");
    //                b.Add("c2value1");
    //            })
    //        }), 0));

    //        var msg = await recieveBuffer.ReceiveAsync();

    //        {
    //            if (msg is StreamMessage<StreamEventBatch> batch)
    //            {
    //                Assert.Equal(1, batch.Data.Events.Count);
    //                Assert.Equal(1, batch.Data.Events[0].Weight);
    //            }
    //        }

    //        await op.SendAsync(new Checkpoint(0, 1));

    //        msg = await recieveBuffer.ReceiveAsync();

    //        await stateManager.InitializeAsync();

    //        op.CreateBlock();
    //        op.Link();

    //        await op.Initialize("test", 0, 0, null, vertexHandler);

    //        await op.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, new List<StreamEvent>()
    //        {
    //            StreamEvent.Create(1, 0, b =>
    //            {
    //                b.Add("c1value1");
    //                b.Add("c2value1");
    //            })
    //        }), 0));

    //        msg = await recieveBuffer.ReceiveAsync();

    //        {
    //            if (msg is StreamMessage<StreamEventBatch> batch)
    //            {
    //                Assert.Equal(1, batch.Data.Events.Count);
    //                Assert.Equal(1, batch.Data.Events[0].Weight);
    //            }
    //        }
    //    }

    //    public void Dispose()
    //    {
    //        if (stateManager != null)
    //        {
    //            stateManager.Dispose();
    //        }
    //    }
    //}
}
