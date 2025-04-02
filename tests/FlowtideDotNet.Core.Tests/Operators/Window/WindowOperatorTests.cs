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
using FlowtideDotNet.Base.Metrics.Internal;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Operators.Window
{
    public class WindowOperatorTests
    {
        WindowOperator _operator;
        BufferBlock<IStreamEvent> _recieveBuffer;

        private async Task InitTests(ConsistentPartitionWindowRelation relation)
        {
            var functionRegister = new Compute.FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functionRegister);

            var executionOptions = new System.Threading.Tasks.Dataflow.ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 100,
                MaxDegreeOfParallelism = 1
            };
            _operator = new WindowOperator(relation, functionRegister, executionOptions);

            var temporaryStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
            {
                DirectoryPath = "./data/temp"
            }, false);

            var stateManagerOptions = new StateManagerOptions()
            {
                PersistentStorage = temporaryStorage,
                CachePageCount = 0,
                UseReadCache = true
            };
            var manager = new StateManagerSync<StateManagerMetadata>(stateManagerOptions, NullLogger.Instance, new System.Diagnostics.Metrics.Meter("tmp"), "test");
            await manager.InitializeAsync();
            var stateClient = manager.GetOrCreateClient("node");

            var dotnetMeter = new System.Diagnostics.Metrics.Meter("tmp");
            var flowtideMeter = new FlowtideMeter(dotnetMeter, new System.Diagnostics.TagList(), () => "node");

            var memoryManager = new OperatorMemoryManager("stream", "node", dotnetMeter);

            var vertexHandler = new VertexHandler(
                    "stream",
                    "node",
                    (span) =>
                    {
                        // Checkpoint
                    },
                    (p1, p2, p3) =>
                    {
                        // Register trigger
                        return Task.CompletedTask;
                    },
                    flowtideMeter,
                    stateClient,
                    NullLoggerFactory.Instance,
                    memoryManager);
            _recieveBuffer = new BufferBlock<IStreamEvent>();

            _operator.LinkTo(_recieveBuffer);

            _operator.Setup("stream", "node");
            _operator.CreateBlock();
            _operator.Link();

            await _operator.Initialize("node", 0, 0,vertexHandler);

            await _operator.SendAsync(new InitWatermarksEvent(new HashSet<string>()
                {
                    "input"
                }));

            var initWatermarkEv = await _recieveBuffer.ReceiveAsync();
        }

        class TestData
        {
            public string? Partition { get; set; }
            public int Value { get; set; }
        }

        [Fact]
        public async Task TestOp()
        {
            var batchConverter = BatchConverter.GetBatchConverter(typeof(TestData));
            
            await InitTests(new ConsistentPartitionWindowRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = batchConverter.GetSchema(),
                    NamedTable = new Substrait.Type.NamedTable()
                    {
                        Names = new List<string>()
                        {
                            "table"
                        }
                    }
                },
                OrderBy = new List<Substrait.Expressions.SortField>(),
                PartitionBy = new List<Substrait.Expressions.Expression>()
                {
                    new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment()
                        {
                            Field = 0
                        }
                    }
                },
                WindowFunctions = new List<Substrait.Expressions.WindowFunction>()
                {
                    new Substrait.Expressions.WindowFunction()
                    {
                        ExtensionName = "sum",
                        ExtensionUri = "/sum.yaml",
                        Arguments = new List<Substrait.Expressions.Expression>()
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
            });

            var eventBatch = batchConverter.ConvertToEventBatch(new List<TestData>()
            {
                new TestData()
                {
                    Value = 1,
                    Partition = "a"
                },
                new TestData()
                {
                    Value = 2,
                    Partition = "a"
                },
                new TestData()
                {
                    Value = 2,
                    Partition = "a"
                },
                new TestData()
                {
                    Value = 4,
                    Partition = "a"
                }
            }, GlobalMemoryManager.Instance);
            PrimitiveList<int> weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            weights.Add(1);
            weights.Add(1);
            weights.Add(1);
            weights.Add(1);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            iterations.Add(0);
            iterations.Add(0);
            iterations.Add(0);
            iterations.Add(0);

            var completion = _operator.Completion.ContinueWith((t) =>
            {
                if (t.IsFaulted)
                {
                    Assert.Fail(t.Exception.Message!);
                }
            });

            await _operator.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new Core.ColumnStore.EventBatchWeighted(weights, iterations, eventBatch)), 0), default);

            await _operator.SendAsync(new Watermark("read", 1));
            var result = await _recieveBuffer.ReceiveAsync();

            if (_operator.Completion.IsFaulted)
            {
                Assert.Fail(_operator.Completion.Exception.Message!);
            }

            var watermarkResult = await _recieveBuffer.ReceiveAsync();

            weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);

            var eventBatch2 = batchConverter.ConvertToEventBatch(new List<TestData>()
            {
                new TestData()
                {
                    Value = 1,
                    Partition = "a"
                },
                new TestData()
                {
                    Value = 2,
                    Partition = "a"
                },
            }, GlobalMemoryManager.Instance);
            weights.Add(-1);
            weights.Add(-1);
            iterations.Add(0);
            iterations.Add(0);

            await _operator.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new Core.ColumnStore.EventBatchWeighted(weights, iterations, eventBatch2)), 0), default);

            await _operator.SendAsync(new Watermark("read", 2));
            var result2 = await _recieveBuffer.ReceiveAsync();

            watermarkResult = await _recieveBuffer.ReceiveAsync();
        }
    }
}
