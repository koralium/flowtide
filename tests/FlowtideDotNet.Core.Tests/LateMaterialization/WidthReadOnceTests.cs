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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Buffer;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class WidthReadOnceTests : OperatorTestBase
{
    private sealed class CountingWidthRelation : Relation
    {
        private readonly ReadRelation _read = new()
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["value"],
                Struct = new Struct { Types = [new Int64Type()] }
            }
        };

        public int Evaluations { get; set; }

        public override int OutputLength
        {
            get
            {
                Evaluations++;
                return _read.OutputLength;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return _read.Accept(visitor, state);
        }
    }

    private sealed class BufferProbe : BufferOperator
    {
        public BufferProbe(BufferRelation relation)
            : base(relation, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 })
        {
        }

        public IAsyncEnumerable<StreamEventBatch> Flush()
        {
            return OnWatermark(new Watermark("t", LongWatermarkValue.Create(1)));
        }
    }

    protected override StateManagerOptions CreateStateManagerOptions()
    {
        return new StateManagerOptions
        {
            PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
        };
    }

    [Fact]
    public async Task APlainBufferDoesNotReevaluateStoreAwareWidthsPerPage()
    {
        var input = new CountingWidthRelation();
        var op = new BufferProbe(new BufferRelation { Input = input });
        await InitializeOperator(op);
        input.Evaluations = 0;

        var allocator = GlobalMemoryManager.Instance;
        var column = Column.Create(allocator);
        column.Add(new Int64Value(42));
        var weights = new PrimitiveList<int>(allocator);
        weights.Add(1);
        var iterations = new PrimitiveList<uint>(allocator);
        iterations.Add(0);
        var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData([column])));
        batch.Data.Rent(1);
        try
        {
            await foreach (var unexpected in op.OnRecieve(batch, 1))
            {
                unexpected.Data.Rent(1);
                unexpected.Data.Return();
                Assert.Fail("The buffer must wait for its watermark.");
            }
        }
        finally
        {
            batch.Data.Return();
        }

        var actual = new List<long>();
        try
        {
            await foreach (var output in op.Flush())
            {
                output.Data.Rent(1);
                try
                {
                    Assert.Equal(1, output.Data.Count);
                    Assert.Equal(1, output.Data.Weights[0]);
                    actual.Add(output.Data.EventBatchData.Columns[0].GetValueAt(0, default).AsLong);
                }
                finally
                {
                    output.Data.Return();
                }
            }
            Assert.Equal([42L], actual);
            Assert.Equal(0, input.Evaluations);
        }
        finally
        {
            await op.DisposeAsync();
        }
    }
}
