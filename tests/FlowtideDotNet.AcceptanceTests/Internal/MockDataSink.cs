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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.StateManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class MockDataSinkState : IStatefulWriteState
    {
        public bool InitialCheckpointDone { get; set; }
        public long StorageSegmentId { get; set; }
    }

    internal class MockDataSink : WriteBaseOperator<MockDataSinkState>
    {
        private readonly Action<List<byte[]>> onDataChange;
        private int crashOnCheckpointCount;
        private SortedDictionary<StreamEvent, int> currentData;

        public MockDataSink(
            ExecutionDataflowBlockOptions executionDataflowBlockOptions, 
            Action<List<byte[]>> onDataChange,
            int crashOnCheckpointCount) : base(executionDataflowBlockOptions)
        {
            currentData = new SortedDictionary<StreamEvent, int>(new BPlusTreeStreamEventComparer());
            this.onDataChange = onDataChange;
            this.crashOnCheckpointCount = crashOnCheckpointCount;
        }

        public override string DisplayName => "Mock Data Sink";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task InitializeOrRestore(long restoreTime, MockDataSinkState? state, IStateManagerClient stateManagerClient)
        {
            
            return Task.CompletedTask;
        }

        protected override Task<MockDataSinkState> OnCheckpoint(long checkpointTime)
        {
            if (crashOnCheckpointCount > 0)
            {
                crashOnCheckpointCount--;
                throw new Exception("Crash on checkpoint");
            }
            if (currentData.Any(x => x.Value < 0))
            {
                Assert.Fail("Row exist in sink with negaive weight");
            }
            var nonDeletedRows = currentData.Where(x => x.Value > 0);
            List<byte[]> output = new List<byte[]>();

            foreach (var row in nonDeletedRows)
            {
                for (int i = 0; i < row.Value; i++)
                {
                    output.Add(row.Key.Memory.ToArray());
                }
            }

            //var actualData = currentData.Where(x => x.Value > 0).Select(x => x.Key.Memory.ToArray()).ToList();
            onDataChange(output);
            return Task.FromResult(new MockDataSinkState());
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            foreach(var e in msg.Events)
            {
                if (currentData.TryGetValue(e, out var weight))
                {
                    currentData[e] = weight + e.Weight;
                }
                else
                {
                    currentData.Add(e, e.Weight);
                }
            }
            return Task.CompletedTask;
        }
    }
}
