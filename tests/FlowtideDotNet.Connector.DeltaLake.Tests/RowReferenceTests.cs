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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using Stowage;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RowReferenceTests
    {
        [Fact]
        public async Task ASinkRefusesRowReferences()
        {
            var testName = nameof(ASinkRefusesRowReferences);
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            var sink = stream.Sinks[0];
            var completion = sink.Completion;
            var allocator = GlobalMemoryManager.Instance;
            var keys = Column.Create(allocator);
            keys.Add(new Int64Value(1));
            var references = Column.Create(allocator);
            references.Add(new RowRefValue(7));
            var weights = new PrimitiveList<int>(allocator);
            weights.Add(1);
            var iterations = new PrimitiveList<uint>(allocator);
            iterations.Add(0);
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(new IColumn[] { keys, references })));
            batch.Data.Rent(1);

            await sink.SendAsync(new StreamMessage<StreamEventBatch>(batch, 1));

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => completion.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("DeltaLakeSink(test) received row references in column 1", error.Message);
        }
    }
}
