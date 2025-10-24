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

using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Debug;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Storage.Memory;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Base.Vertices.Egress;
using System.Collections.Immutable;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Base class for testing operators
    /// </summary>
    public abstract class OperatorTestBase : IDisposable
    {
        private TaskCompletionSource? _taskCompletion;
        private StateManagerSync<StreamState>? _stateManager;
        internal FunctionsRegister FunctionsRegister { get; private set; }

        public OperatorTestBase()
        {
            FunctionsRegister = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(FunctionsRegister);
        }

        public async Task InitializeOperator(IStreamVertex @operator)
        {
            @operator.Setup("stream", "1");
            @operator.CreateBlock();
            @operator.Link();
            
            var statemanagermeter = new Meter("statemanager");
            _stateManager = new StateManagerSync<StreamState>(new StateManagerOptions()
            {
            }, new DebugLoggerProvider().CreateLogger("state"), statemanagermeter, "stream");
            await _stateManager.InitializeAsync();
            await ReinitializeOperator(@operator);
        }

        public async Task ReinitializeOperator(IStreamVertex @operator)
        {
            if (_stateManager == null)
            {
                throw new InvalidOperationException("State manager is not initialized");
            }

            var metrics = new StreamMetrics("stream");
            var vertexHandler = new VertexHandler(
                "stream",
                "1",
                (time) =>
                {

                },
                (p1, p2, t) => Task.CompletedTask,
                metrics.GetOrCreateVertexMeter("1", () => ""),
                _stateManager.GetOrCreateClient("1"),
                new LoggerFactory(),
                new OperatorMemoryManager("stream", "op", new Meter("stream")),
                (exception, version) => Task.CompletedTask);
            await @operator.Initialize("1", 0, 0, vertexHandler, null);

            if (@operator is IStreamEgressVertex egressVertex)
            {
                egressVertex.SetCheckpointDoneFunction((name) =>
                {
                    if (_taskCompletion != null)
                    {
                        _taskCompletion.SetResult();
                    }
                }, (name) => { });
            }
        }

        public void ClearCache()
        {
            if (_stateManager != null)
            {
                _stateManager.ClearCache();
            }
        }

        public async Task SendWatermarkInitialize(ITargetBlock<IStreamEvent> target, IReadOnlySet<string> watermarkNames)
        {
            await target.SendAsync(new InitWatermarksEvent(watermarkNames));
        }

        public async Task SendDataBatchWithWeightOne<T>(ITargetBlock<IStreamEvent> target, IEnumerable<T> data)
        {
            var converter = BatchConverter.GetBatchConverter(typeof(T));
            var eventBatch = converter.ConvertToEventBatch(data, GlobalMemoryManager.Instance);

            PrimitiveList<int> weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);

            var dataCount = data.Count();

            for(int i = 0; i < dataCount; i++)
            {
                weights.Add(1);
                iterations.Add(0);
            }

            await target.SendAsync(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new Core.ColumnStore.EventBatchWeighted(weights, iterations, eventBatch)), 1));
        }

        public async Task SendCheckpoint(ITargetBlock<IStreamEvent> target, long checkpointTime)
        {
            _taskCompletion = new TaskCompletionSource();
            await target.SendAsync(new Checkpoint(checkpointTime, checkpointTime + 1));
        }

        public async Task SendWatermark(ITargetBlock<IStreamEvent> target, IReadOnlyDictionary<string, AbstractWatermarkValue> values)
        {
            await target.SendAsync(new Watermark(values.ToImmutableDictionary()));
        }

        /// <summary>
        /// This method only functions if testing an egress operator
        /// </summary>
        /// <returns></returns>
        public async Task EgressWaitForCheckpointDone()
        {
            if (_taskCompletion != null)
            {
                await _taskCompletion.Task;
            }
        }

        public void Dispose()
        {
            if (_stateManager != null)
            {
                _stateManager.Dispose();
            }
            
        }
    }
}
