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
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Serialization
{
    public class UnionNullCounterRoundtripTests
    {
        private static EventBatchData Roundtrip(EventBatchData batch)
        {
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, batch.Count);

            var deserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance);
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bufferWriter.WrittenMemory));
            return deserializer.DeserializeBatch(ref reader).EventBatch;
        }

        private static string[] ReadRows(EventBatchData batch)
        {
            return Enumerable.Range(0, batch.Count)
                .Select(i => batch.Columns[0].GetValueAt(i, default).ToString() ?? "<null>")
                .ToArray();
        }

        /// <summary>
        /// A struct field holding mixed types becomes a union, and a null struct row pushes a null into it.
        /// </summary>
        [Fact]
        public void StructWithUnionFieldAndNullRowSurvivesRoundtrip()
        {
            var header = StructHeader.Create("a");
            var column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StructValue(header, new Int64Value(1)));
            column.Add(new StructValue(header, new StringValue("two")));
            column.Add(NullValue.Instance);
            column.Add(new StructValue(header, new Int64Value(3)));

            using var batch = new EventBatchData([column]);
            var before = ReadRows(batch);

            using var restored = Roundtrip(batch);
            var after = ReadRows(restored);

            Assert.Equal(before, after);
        }

        /// <summary>
        /// The union encodes its own nulls in the type list, so the wrapping column must never count them.
        /// </summary>
        [Fact]
        public void UnionColumnInsideStructKeepsZeroNullCounter()
        {
            var header = StructHeader.Create("a");
            var column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StructValue(header, new Int64Value(1)));
            column.Add(new StructValue(header, new StringValue("two")));
            column.Add(NullValue.Instance);

            var structColumn = (StructColumn)((IColumn)column).DataColumn;
            var inner = structColumn._columns[0];

            Assert.Equal(ArrowTypeId.Union, inner.Type);
            Assert.Equal(0, inner.NullCounter);
        }

        /// <summary>
        /// Null padding arrives as a range from a null column, which is the path that reaches StructColumn.InsertNullRange.
        /// </summary>
        [Fact]
        public void NullRangeIntoStructWithUnionFieldKeepsZeroNullCounter()
        {
            var header = StructHeader.Create("a");
            var column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StructValue(header, new Int64Value(1)));
            column.Add(new StructValue(header, new StringValue("two")));

            var nulls = Column.Create(GlobalMemoryManager.Instance);
            nulls.Add(NullValue.Instance);
            nulls.Add(NullValue.Instance);

            column.InsertRangeFrom(2, nulls, 0, 2);

            var structColumn = (StructColumn)((IColumn)column).DataColumn;
            var inner = structColumn._columns[0];

            Assert.Equal(ArrowTypeId.Union, inner.Type);
            Assert.Equal(0, inner.NullCounter);
        }

        /// <summary>
        /// Same shape as the padding case, but checked end to end through a serialize and restore.
        /// </summary>
        [Fact]
        public void NullRangeIntoStructWithUnionFieldSurvivesRoundtrip()
        {
            var header = StructHeader.Create("a");
            var column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StructValue(header, new Int64Value(1)));
            column.Add(new StructValue(header, new StringValue("two")));

            var nulls = Column.Create(GlobalMemoryManager.Instance);
            nulls.Add(NullValue.Instance);

            column.InsertRangeFrom(2, nulls, 0, 1);
            column.Add(new StructValue(header, new Int64Value(3)));

            using var batch = new EventBatchData([column]);
            var before = ReadRows(batch);

            using var restored = Roundtrip(batch);
            var after = ReadRows(restored);

            Assert.Equal(before, after);
        }
    }
}
