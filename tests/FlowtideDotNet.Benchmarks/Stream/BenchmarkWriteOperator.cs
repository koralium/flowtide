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

using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Benchmarks.Stream
{
    public class BenchmarkWriteOperatorFactory : RegexConnectorSinkFactory
    {
        private readonly Action onCheckpoint;

        public BenchmarkWriteOperatorFactory(string regexPattern, Action onCheckpoint) : base(regexPattern)
        {
            this.onCheckpoint = onCheckpoint;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new BenchmarkWriteOperator(dataflowBlockOptions, onCheckpoint);
        }
    }

    internal class BenchmarkWriteOperator : WriteBaseOperator
    {
        private readonly Action onCheckpoint;

        public BenchmarkWriteOperator(ExecutionDataflowBlockOptions executionDataflowBlockOptions, Action onCheckpoint) : base(executionDataflowBlockOptions)
        {
            this.onCheckpoint = onCheckpoint;
        }

        public override string DisplayName => "Benchmark Sink";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            onCheckpoint();
            return Task.CompletedTask;
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            return Task.CompletedTask;
        }
    }
}
