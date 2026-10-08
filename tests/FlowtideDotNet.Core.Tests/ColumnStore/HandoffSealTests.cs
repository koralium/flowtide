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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class HandoffSealTests
    {
        private static Column IntColumn(params long[] values)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            foreach (var value in values)
            {
                column.Add(new Int64Value(value));
            }
            return column;
        }

        private static StreamEventBatch Send(params IColumn[] columns)
        {
            var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int i = 0; i < columns[0].Count; i++)
            {
                weights.Add(1);
                iterations.Add(0);
            }
            return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
        }

        private static void Mutate(Column column, string mutator)
        {
            switch (mutator)
            {
                case "Add":
                    column.Add(new Int64Value(4));
                    break;
                case "InsertAt":
                    column.InsertAt(0, new Int64Value(4));
                    break;
                case "UpdateAt":
                    column.UpdateAt(0, new Int64Value(4));
                    break;
                case "RemoveAt":
                    column.RemoveAt(0);
                    break;
                case "RemoveRange":
                    column.RemoveRange(0, 1);
                    break;
                case "Clear":
                    column.Clear();
                    break;
                case "InsertNullRange":
                    column.InsertNullRange(0, 1);
                    break;
                case "InsertRangeFrom":
                    column.InsertRangeFrom(0, IntColumn(9), 0, 1);
                    break;
                case "InsertFrom":
                    ReadOnlySpan<int> lookup = [0];
                    ReadOnlySpan<int> positions = [0];
                    column.InsertFrom(IntColumn(9), in lookup, in positions, -1);
                    break;
                case "DeleteBatch":
                    column.DeleteBatch([0]);
                    break;
                case "AddToNewList":
                    column.AddToNewList(new Int64Value(4));
                    break;
                case "EndNewList":
                    column.EndNewList();
                    break;
                default:
                    throw new ArgumentException(mutator);
            }
        }

        [Theory]
        [InlineData("Add")]
        [InlineData("InsertAt")]
        [InlineData("UpdateAt")]
        [InlineData("RemoveAt")]
        [InlineData("RemoveRange")]
        [InlineData("Clear")]
        [InlineData("InsertNullRange")]
        [InlineData("InsertRangeFrom")]
        [InlineData("InsertFrom")]
        [InlineData("DeleteBatch")]
        [InlineData("AddToNewList")]
        [InlineData("EndNewList")]
        public void EveryColumnMutatorThrowsAfterSend(string mutator)
        {
            var column = IntColumn(1, 2, 3);
            Send(column);

            var ex = Assert.Throws<InvalidOperationException>(() => Mutate(column, mutator));

            Assert.Contains($"Column.{mutator} was called on data that was already handed downstream", ex.Message);
        }

        /// <summary>
        /// A union reallocates its offsets on append, the case that corrupts a concurrent reader.
        /// </summary>
        [Fact]
        public void UnionColumnThrowsAfterSend()
        {
            var column = IntColumn(1);
            column.Add(new StringValue("two"));
            Assert.Equal(ArrowTypeId.Union, column.Type);
            Send(column);

            Assert.Throws<InvalidOperationException>(() => column.Add(new StringValue("three")));
        }

        [Fact]
        public void WeightsAndIterationsThrowAfterSend()
        {
            var batch = Send(IntColumn(1));

            Assert.Throws<InvalidOperationException>(() => batch.Data.Weights.Add(1));
            Assert.Throws<InvalidOperationException>(() => batch.Data.Weights[0] = 5);
            Assert.Throws<InvalidOperationException>(() => batch.Data.Iterations.Add(0));
        }

        [Fact]
        public void ColumnWithOffsetSealsBothOffsetsAndInnerColumn()
        {
            var inner = IntColumn(1, 2);
            var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(1);
            offsets.Add(0);
            Send(new ColumnWithOffset(inner, offsets));

            Assert.Throws<InvalidOperationException>(() => inner.Add(new Int64Value(3)));
            Assert.Throws<InvalidOperationException>(() => offsets.Add(0));
        }

        [Fact]
        public void WritesBeforeSendAreAllowed()
        {
            var column = IntColumn(1, 2);
            column.RemoveAt(0);
            column.Add(new Int64Value(3));

            Assert.Equal(2, column.Count);
        }

        [Fact]
        public void TheLastOwnerCanStillDisposeAfterSend()
        {
            var batch = Send(IntColumn(1, 2));

            batch.Data.EventBatchData.Dispose();
            batch.Data.Weights.Dispose();
            batch.Data.Iterations.Dispose();
        }

        [Fact]
        public void AssignClearsTheSeal()
        {
            var column = IntColumn(1);
            Send(column);

            column.Assign(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(2));

            Assert.Equal(1, column.Count);
        }
    }
}
