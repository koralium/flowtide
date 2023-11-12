//// Licensed under the Apache License, Version 2.0 (the "License")
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////  
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Linq;
//using System.Text;
//using System.Text.Json;
//using System.Threading.Tasks;
//using System.Threading.Tasks.Dataflow;
//using FlowtideDotNet.Base.Engine.Internal;
//using FlowtideDotNet.Base.Vertices.Unary;

//namespace FlowtideDotNet.Base.Vertices.PartitionVertices
//{
//    internal abstract class PartitionedUnaryVertex<T, TState> : IPropagatorBlock<IStreamEvent, IStreamEvent>, IStreamVertex
//    {
//        private UnaryVertex<T, TState>[]? _vertices;
//        private TransformManyBlock<IStreamEvent, KeyValuePair<int, IStreamEvent>>? _inputBlock;
//        private ITargetBlock<IStreamEvent>? _inputTargetBlock;
//        private List<TransformBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>>? _parallelBlocks;
//        private ISourceBlock<IStreamEvent>? _sourceBlock;
//        private PartitionedOutputVertex<T, TState>? _partitionedOutputVertex;

//        public PartitionedUnaryVertex()
//        {
            
//        }

//        public Task Completion => GetCompletionTask();

//        private Task GetCompletionTask()
//        {
//            Debug.Assert(_vertices != null);
//            Task[] tasks = new Task[_vertices.Length];
//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                tasks[i] = _vertices[i].Completion;
//            }
//            return Task.WhenAll(tasks);
//        }

//        public string Name => throw new NotImplementedException();

//        public string DisplayName => throw new NotImplementedException();

//        public Task Compact()
//        {
//            Debug.Assert(_vertices != null);
//            Task[] tasks = new Task[_vertices.Length];
//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                tasks[i] = _vertices[i].Compact();
//            }
//            return Task.WhenAll(tasks);
//        }

//        protected abstract UnaryVertex<T, TState>[] GetVertices();

//        public void Complete()
//        {
//            Debug.Assert(_vertices != null);
//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                _vertices[i].Complete();
//            }
//        }

//        public IStreamEvent? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target, out bool messageConsumed)
//        {
//            Debug.Assert(_sourceBlock != null);
//            return _sourceBlock.ConsumeMessage(messageHeader, target, out messageConsumed);
//        }

//        public void CreateBlock()
//        {
//            Debug.Assert(_vertices == null);
//            Debug.Assert(_partitionedOutputVertex != null);

//            _partitionedOutputVertex.CreateBlock();

//            _inputBlock = new TransformManyBlock<IStreamEvent, KeyValuePair<int, IStreamEvent>>(x =>
//            {
//                if (x is StreamMessage<T> message)
//                {

//                }
//            }, new ExecutionDataflowBlockOptions()
//            {
//                MaxDegreeOfParallelism = 1
//            });
//            _inputTargetBlock = _inputBlock;
//            _parallelBlocks = new List<TransformBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>>();

//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                int index = i;
//                var linkBlock = new TransformBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>(x => x.Value);
//                _inputBlock.LinkTo(linkBlock, new DataflowLinkOptions() { PropagateCompletion = true }, x => x.Key == index);
//                _parallelBlocks.Add(linkBlock);
//            }
//            _sourceBlock = _partitionedOutputVertex;

//        }

//        public Task DeleteAsync()
//        {
//            Debug.Assert(_vertices != null);
//            Task[] tasks = new Task[_vertices.Length];
//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                tasks[i] = _vertices[i].DeleteAsync();
//            }
//            return Task.WhenAll(tasks);
//        }

//        public async ValueTask DisposeAsync()
//        {
//            Debug.Assert(_vertices != null);
//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                await _vertices[i].DisposeAsync();
//            }
//        }

//        public void Fault(Exception exception)
//        {
//            Debug.Assert(_vertices != null);
//            Debug.Assert(_inputBlock != null);

//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                _vertices[i].Fault(exception);
//            }
//            (_inputBlock as IDataflowBlock).Fault(exception);
//        }

//        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
//        {
//            Debug.Assert(_partitionedOutputVertex != null);
//            return _partitionedOutputVertex.GetLinks();
//        }

//        public async Task Initialize(string name, long restoreTime, long newTime, JsonElement? state, IVertexHandler vertexHandler)
//        {
//            Debug.Assert(_vertices != null);
//            Debug.Assert(_partitionedOutputVertex != null);

//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                var childVertexHandler = new VertexHandler(
//                    vertexHandler.StreamName,
//                    $"{name}_{i}",
//                    vertexHandler.ScheduleCheckpoint,
//                    (n, a, b) => vertexHandler.RegisterTrigger(a, b),
//                    vertexHandler.Metrics,
//                    vertexHandler.StateClient.GetChildManager(i.ToString()),
//                    vertexHandler.LoggerFactory);
                    
//                await _vertices[i].Initialize($"{name}_{i}", restoreTime, newTime, default, childVertexHandler);
//            }
//            _partitionedOutputVertex.Link();
//        }

//        public void Link()
//        {
//            Debug.Assert(_vertices != null);
//            Debug.Assert(_partitionedOutputVertex != null);

//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                _vertices[i].Link();
//            }
//            _partitionedOutputVertex.Link();
//        }

//        public IDisposable LinkTo(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
//        {
//            Debug.Assert(_partitionedOutputVertex != null);
//            return _partitionedOutputVertex.LinkTo(target, linkOptions);
//        }

//        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, IStreamEvent messageValue, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
//        {
//            Debug.Assert(_inputTargetBlock != null);
//            return _inputTargetBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
//        }

//        public Task QueueTrigger(TriggerEvent triggerEvent)
//        {
            
//            return Task.CompletedTask;
//        }

//        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
//        {
//            Debug.Assert(_sourceBlock != null);
//            _sourceBlock.ReleaseReservation(messageHeader, target);
//        }

//        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
//        {
//            Debug.Assert(_sourceBlock != null);
//            return _sourceBlock.ReserveMessage(messageHeader, target);
//        }

//        public void Setup(string streamName, string operatorName)
//        {
//            _vertices = GetVertices();
//            _partitionedOutputVertex = new PartitionedOutputVertex<T, TState>(_vertices.Length, new ExecutionDataflowBlockOptions()
//            {
//                MaxDegreeOfParallelism = 1,
//                BoundedCapacity = 100
//            });

//            _partitionedOutputVertex.Setup(streamName, $"{operatorName}_output");
//            for (int i = 0; i < _vertices.Length; i++)
//            {
//                // Link each parallell vertex to input target of partioned output
//                _vertices[i].LinkTo(_partitionedOutputVertex.Targets[i], new DataflowLinkOptions() { PropagateCompletion = true });
//                _vertices[i].Setup(streamName, $"{operatorName}_{i}");
//            }
//        }
//    }
//}
