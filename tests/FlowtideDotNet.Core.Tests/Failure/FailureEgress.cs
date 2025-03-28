﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Failure
{
    internal class FailureEgressOptions
    {
        public Action? OnCheckpoint { get; set; }

        public Action? OnCompaction { get; set; }
    }

    internal class FailureEgressFactory : RegexConnectorSinkFactory
    {
        private readonly FailureEgressOptions failureEgressOptions;

        public FailureEgressFactory(string regexPattern, FailureEgressOptions failureEgressOptions) : base(regexPattern)
        {
            this.failureEgressOptions = failureEgressOptions;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new FailureEgress(dataflowBlockOptions, failureEgressOptions);
        }
    }

    internal class FailureEgress : GroupedWriteBaseOperator
    {
        private readonly FailureEgressOptions failureEgressOptions;
        List<int> primaryKeyIds;
        public FailureEgress(
            ExecutionDataflowBlockOptions executionDataflowBlockOptions,
            FailureEgressOptions failureEgressOptions) : base(executionDataflowBlockOptions)
        {
            primaryKeyIds = new List<int>();
            primaryKeyIds.Add(0);
            this.failureEgressOptions = failureEgressOptions;
        }

        public override Task Compact()
        {
            failureEgressOptions.OnCompaction?.Invoke();
            return base.Compact();
        }

        public override string DisplayName => "FailureEgress";

        protected override Task Checkpoint(long checkpointTime)
        {
            failureEgressOptions.OnCheckpoint?.Invoke();
            return Task.CompletedTask;
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult<IReadOnlyList<int>>(primaryKeyIds);
        }

        protected override Task Initialize(long restoreTime, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            return Task.CompletedTask;
        }
    }
}
