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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinNestedCopyTests
    {
        public static IEnumerable<object[]> Kinds()
        {
            yield return new object[] { "list" };
            yield return new object[] { "map" };
            yield return new object[] { "struct" };
            yield return new object[] { "structOfMixed" };
        }

        private static IDataValue Value(string kind, int i, StructHeader header)
        {
            return kind switch
            {
                "list" => new ListValue(new Int64Value(i), new StringValue("v" + i)),
                "map" => new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("k" + i), new Int64Value(i))),
                "struct" => new StructValue(header, new Int64Value(i)),
                _ => new StructValue(header, i % 2 == 0 ? new Int64Value(i) : new DoubleValue(i + 0.5))
            };
        }

        [Theory]
        [MemberData(nameof(Kinds))]
        public void NestedRowsCopiedInRunsKeepTheirValues(string kind)
        {
            var header = StructHeader.Create("x");
            using var source = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 6; i++)
            {
                source.Add(Value(kind, i, header));
            }
            // Scattered, repeated and null rows, as a lookup through a secondary index with repeated keys gives them
            int[] rows = { 4, 5, 0, 0, 1, ColumnWithOffset.NullValueIndex, ColumnWithOffset.NullValueIndex, 5, 2, 3 };
            using var target = Column.Create(GlobalMemoryManager.Instance);
            // A first part from another leaf, so the copy appends
            MultiJoinTreeLookup.CopyRuns(target, source, new[] { 3 });
            MultiJoinTreeLookup.CopyRuns(target, source, rows);

            Assert.Equal(rows.Length + 1, target.Count);
            Assert.Equal(0, DataValueComparer.CompareTo(source.GetValueAt(3, default), target.GetValueAt(0, default)));
            for (int r = 0; r < rows.Length; r++)
            {
                IDataValue expected = rows[r] < 0 ? NullValue.Instance : source.GetValueAt(rows[r], default);
                var actual = target.GetValueAt(r + 1, default);
                Assert.True(DataValueComparer.CompareTo(expected, actual) == 0, $"{kind} row {r}: expected {expected}, got {actual}");
            }
        }
    }
}
