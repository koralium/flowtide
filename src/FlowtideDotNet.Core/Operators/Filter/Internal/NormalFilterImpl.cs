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

using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.Filter.Internal
{
    internal class NormalFilterImpl : IFilterImplementation
    {
        private readonly Func<StreamEvent, bool> _expression;
        public NormalFilterImpl(FilterRelation filterRelation, FunctionsRegister functionsRegister)
        {
            _expression = BooleanCompiler.Compile<StreamEvent>(filterRelation.Condition, functionsRegister);
        }

        public Task Compact()
        {
            return Task.CompletedTask;
        }

        public Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public Task InitializeOrRestore(string streamName, string operatorName, Func<string, TimeSpan?, Task> addTriggerFunc, object? state)
        {
            return Task.CompletedTask;
        }

        public Task<object?> OnCheckpoint()
        {
            return Task.FromResult<object?>(null);
        }

        public async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            List<StreamEvent> output = new List<StreamEvent>();

            foreach (var e in msg.Events)
            {
                if (_expression(e))
                {
                    output.Add(e);
                }
            }

            if (output.Count > 0)
            {
                yield return new StreamEventBatch(null, output);
            }
        }

        public async IAsyncEnumerable<StreamEventBatch> OnTrigger(string triggerName, object? state)
        {
            yield break;
        }
    }
}
