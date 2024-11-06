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

using FlexBuffers;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Project
{
    internal class ColumnProjectOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private readonly ProjectRelation projectRelation;
        private readonly Action<EventBatchData, int, Column>[] _expressions;
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public ColumnProjectOperator(ProjectRelation projectRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.projectRelation = projectRelation;
            _expressions = new Action<EventBatchData, int, Column>[projectRelation.Expressions.Count];
            for (int i = 0; i < _expressions.Length; i++)
            {
                _expressions[i] = ColumnProjectCompiler.Compile(projectRelation.Expressions[i], functionsRegister);
            }
        }

        public override string DisplayName => "Projection";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<object?> OnCheckpoint()
        {
            return Task.FromResult<object?>(default);
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsCounter != null);
            var data = msg.Data;

            _eventsProcessed.Add(data.Count);
            _eventsCounter.Add(data.Count);

            Column[] projectionColumns = new Column[_expressions.Length];

            for (int i = 0; i < _expressions.Length; i++)
            {
                projectionColumns[i] = ColumnFactory.Get(MemoryAllocator);
            }

            for (int i = 0; i < data.Count; i++)
            {
                for (int k = 0; k < _expressions.Length; k++)
                {
                    _expressions[k](data.EventBatchData, i, projectionColumns[k]);
                }
            }
                
            if (projectRelation.EmitSet)
            {
                IColumn[] outputColumns = new IColumn[projectRelation.OutputLength];

                for (int i = 0; i < projectRelation.Emit.Count; i++)
                {
                    var emitVal = projectRelation.Emit[i];
                    if (emitVal >= projectRelation.Input.OutputLength)
                    {
                        outputColumns[i] = projectionColumns[emitVal - projectRelation.Input.OutputLength];
                    }
                    else
                    {
                        outputColumns[i] = data.EventBatchData.Columns[emitVal];
                    }
                }

                var newBatch = new EventBatchData(outputColumns);
                return new SingleAsyncEnumerable<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(data.Weights, data.Iterations, newBatch)));
            }
            else
            {
                IColumn[] outputColumns = new IColumn[projectRelation.OutputLength];
                for (int i = 0; i < data.EventBatchData.Columns.Count; i++)
                {
                    outputColumns[i] = data.EventBatchData.Columns[i];
                }
                for (int i = 0; i < projectionColumns.Length; i++)
                {
                    outputColumns[i + data.EventBatchData.Columns.Count] = projectionColumns[i];
                }
                var newBatch = new EventBatchData(outputColumns);
                return new SingleAsyncEnumerable<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(data.Weights, data.Iterations, newBatch)));
            }
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            Logger.InitializingProjectOperator(StreamName, Name);
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            return Task.CompletedTask;
        }
    }
}
