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

using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute.Unwrap;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Unwrap
{
    internal class UnwrapOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private readonly UnwrapRelation unwrapRelation;
        private readonly Func<RowEvent, bool>? _filter;
        private readonly Func<RowEvent, FlexBuffers.FlxValue> _fieldProjectFunc;
        private readonly Func<FlexBuffers.FlxValue, IReadOnlyList<IReadOnlyList<FlexBuffers.FlxValue>>> _unwrapFunc;

        public override string DisplayName => "Unwrap";

        public UnwrapOperator(UnwrapRelation unwrapRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionDataflowBlockOptions)
        {
            this.unwrapRelation = unwrapRelation;

            if (unwrapRelation.Filter != null)
            {
                _filter = BooleanCompiler.Compile<RowEvent>(unwrapRelation.Filter, functionsRegister);
            }
            _fieldProjectFunc = ProjectCompiler.Compile(unwrapRelation.Field, functionsRegister);
            _unwrapFunc = UnwrapCompiler.CompileUnwrap(unwrapRelation.BaseSchema.Names);
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

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            List<RowEvent> output = new List<RowEvent>();
            foreach(var e in msg.Events)
            {
                var valueToUnwrap = _fieldProjectFunc(e);

                var unwrapRows = _unwrapFunc(valueToUnwrap);

                foreach(var unwrapRow in unwrapRows)
                {
                    var filterEvent = RowEvent.Create(e.Weight, 0, b =>
                    {
                        for (int i = 0; i < e.Length; i++)
                        {
                            b.Add(e.GetColumn(i));
                        }
                        for (int i = 0; i < unwrapRow.Count; i++)
                        {
                            b.Add(unwrapRow[i]);
                        }
                    });
                    var projectedEvent = RowEvent.Create(e.Weight, 0, b =>
                    {
                        if (unwrapRelation.EmitSet)
                        {
                            for (int i = 0; i < unwrapRelation.Emit!.Count; i++)
                            {
                                var index = unwrapRelation.Emit[i];
                                if (index >= e.Length)
                                {
                                    b.Add(unwrapRow[index - e.Length]);
                                }
                                else
                                {
                                    b.Add(e.GetColumn(index));
                                }
                            }
                        }
                        else
                        {
                            for (int i = 0; i < e.Length; i++)
                            {
                                b.Add(e.GetColumn(i));
                            }
                            for (int i = 0; i < unwrapRow.Count; i++)
                            {
                                b.Add(unwrapRow[i]);
                            }
                        }
                    });

                    if (_filter != null)
                    {
                        if (_filter(filterEvent))
                        {
                            output.Add(projectedEvent);
                        }
                    }
                    else
                    {
                        output.Add(projectedEvent);
                    }
                }
            }

            if (output.Count > 0)
            {
                return new SingleAsyncEnumerable<StreamEventBatch>(new StreamEventBatch(output, unwrapRelation.OutputLength));
            }
            return EmptyAsyncEnumerable<StreamEventBatch>.Instance;
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
