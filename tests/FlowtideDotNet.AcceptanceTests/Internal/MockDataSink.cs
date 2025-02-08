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
using FlowtideDotNet.Base;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.StateManager;
using System.Buffers;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class MockDataSink : WriteBaseOperator
    {
        private readonly Action<List<byte[]>> onDataChange;
        private int crashOnCheckpointCount;
        private SortedDictionary<RowEvent, int> currentData;
        private bool watermarkRecieved = false;
        private Action<Watermark> onWatermark;
        public MockDataSink(
            ExecutionDataflowBlockOptions executionDataflowBlockOptions, 
            Action<List<byte[]>> onDataChange,
            int crashOnCheckpointCount,
            Action<Watermark> onWatermark) : base(executionDataflowBlockOptions)
        {
            currentData = new SortedDictionary<RowEvent, int>(new BPlusTreeStreamEventComparer());
            this.onDataChange = onDataChange;
            this.crashOnCheckpointCount = crashOnCheckpointCount;
            this.onWatermark = onWatermark;
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

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            
            return Task.CompletedTask;
        }

        protected override Task OnWatermark(Watermark watermark)
        {
            watermarkRecieved = true;
            if (onWatermark != null)
            {
                onWatermark(watermark);
            }
            return base.OnWatermark(watermark);
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            if (crashOnCheckpointCount > 0)
            {
                crashOnCheckpointCount--;
                currentData.Clear();
                throw new CrashException("Crash on checkpoint");
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
                    var compactData = (CompactRowData)row.Key.Compact(new FlexBuffer(ArrayPool<byte>.Shared)).RowData;
                    output.Add(compactData.Span.ToArray());
                }
            }

            if (watermarkRecieved)
            {
                onDataChange(output);
                watermarkRecieved = false;
            }
            
            return Task.CompletedTask;
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
