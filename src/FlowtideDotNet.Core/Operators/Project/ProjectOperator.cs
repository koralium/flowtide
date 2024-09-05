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

using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Storage.StateManager;
using FlexBuffers;
using Microsoft.Extensions.Logging;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.Utils;

namespace FlowtideDotNet.Core.Operators.Project
{
    /// <summary>
    /// Project operator takes in events and applies a list of expressions to add new columns based on the expressions.
    /// </summary>
    internal class ProjectOperator : UnaryVertex<StreamEventBatch, object?>
    {
#if DEBUG_WRITE
        private StreamWriter allInput;
#endif
        private readonly ProjectRelation projectRelation;
        private readonly Func<RowEvent, FlxValue>[] _expressions;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public override string DisplayName => "Projection";

        public ProjectOperator(ProjectRelation projectRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _expressions = new Func<RowEvent, FlxValue>[projectRelation.Expressions.Count];
            
            for (int i = 0; i < _expressions.Length; i++)
            {
                _expressions[i] = ProjectCompiler.Compile(projectRelation.Expressions[i], functionsRegister);
            }

            this.projectRelation = projectRelation;
        }

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
            return Task.FromResult<object?>(null);
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(msg.Events.Count);
            List<RowEvent> output = new List<RowEvent>();

            foreach (var e in msg.Events)
            {
#if DEBUG_WRITE
                allInput.WriteLine($"Input: {e.Weight} {e.ToJson()}");
#endif
                FlxValue[] extraFelds = new FlxValue[_expressions.Length];

                for(int i = 0; i < _expressions.Length; i++)
                {
                    extraFelds[i] = _expressions[i](e);
                }

                if (projectRelation.EmitSet)
                {
                    FlxValue[] newVector = new FlxValue[projectRelation.Emit.Count];
                    for (int i = 0; i < projectRelation.Emit.Count; i++)
                    {
                        var index = projectRelation.Emit[i];
                        if (index >= e.Length)
                        {
                            newVector[i] = extraFelds[index - e.Length];
                        }
                        else
                        {
                            newVector[i] = e.GetColumn(index);
                        }
                    }
                    output.Add(new RowEvent(e.Weight, e.Iteration, new ArrayRowData(newVector)));
                }
                else
                {
                    FlxValue[] newVector = new FlxValue[e.Length + extraFelds.Length];
                    for (int i = 0; i < e.Length; i++)
                    {
                        newVector[i] = e.GetColumn(i);
                    }
                    for (int i = 0; i < extraFelds.Length; i++)
                    {
                        newVector[i + e.Length] = extraFelds[i];
                    }
                    output.Add(new RowEvent(e.Weight, e.Iteration, new ArrayRowData(newVector)));
                }
            }

#if DEBUG_WRITE
            await allInput.FlushAsync();
#endif
            if (output.Count > 0)
            {
                Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));
                _eventsCounter.Add(output.Count);
                yield return new StreamEventBatch(output, projectRelation.OutputLength);
            }
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            if (!Directory.Exists("debugwrite"))
            {
                var dir = Directory.CreateDirectory("debugwrite");
            }
            if(allInput == null)
            {
                allInput = File.CreateText($"debugwrite/{StreamName}_{Name}.all.txt");
            }
            else
            {
                allInput.WriteLine("Restart");
                allInput.Flush();
            }
            
#endif
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
