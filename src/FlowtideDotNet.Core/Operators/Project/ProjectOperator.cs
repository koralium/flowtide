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
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;

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
        private readonly Func<StreamEvent, FlxValue>[] _expressions;

        private Counter<long>? _eventsCounter;

        public override string DisplayName => "Projection";

        public ProjectOperator(ProjectRelation projectRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _expressions = new Func<StreamEvent, FlxValue>[projectRelation.Expressions.Count];
            
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
            List<StreamEvent> output = new List<StreamEvent>();

            foreach (var e in msg.Events)
            {
#if DEBUG_WRITE
                allInput.WriteLine($"Input: {e.Weight} {e.Vector.ToJson}");
#endif
                FlxValue[] extraFelds = new FlxValue[_expressions.Length];

                for(int i = 0; i < _expressions.Length; i++)
                {
                    extraFelds[i] = _expressions[i](e);
                }
                var projectedEvent = StreamEvent.Create(e.Weight, 0, b =>
                {
                    var vectorSpan = e.Vector.Span;
                    if (projectRelation.EmitSet)
                    {
                        for (int i = 0; i < projectRelation.Emit!.Count; i++)
                        {
                            var index = projectRelation.Emit[i];
                            if (index >= e.Vector.Length)
                            {
                                b.Add(extraFelds[index - e.Vector.Length]);
                            }
                            else
                            {
                                b.Add(e.Vector.GetWithSpan(index, vectorSpan));
                            }
                        }
                    }
                    else
                    {
                        for(int i = 0; i < e.Vector.Length; i++)
                        {
                            b.Add(e.Vector.GetWithSpan(i, vectorSpan));
                        }
                        for (int i = 0; i < extraFelds.Length; i++)
                        {
                            b.Add(extraFelds[i]);
                        }
                    }
                });
                output.Add(projectedEvent);
            }

#if DEBUG_WRITE
            await allInput.FlushAsync();
#endif
            if (output.Count > 0)
            {
                Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));
                _eventsCounter.Add(output.Count);
                yield return new StreamEventBatch(null, output);
            }
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            allInput = File.CreateText($"{Name}.all.txt");
#endif
            Logger.LogInformation("Initializing project operator.");
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            
            return Task.CompletedTask;
        }
    }
}
