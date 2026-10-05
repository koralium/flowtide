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
using FlowtideDotNet.Storage.Memory;
using System.Text;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class ColumnMemoryManagerTests
    {
        [Fact]
        public void StringValueSurvivesColumnRealloc()
        {
            var column = new StringColumn(GlobalMemoryManager.Instance);
            column.Add(new StringValue("hello world"), GlobalMemoryManager.Instance);

            var memory = column.GetValueAt(0, default).AsString.Memory;
            Assert.Equal("hello world", Encoding.UTF8.GetString(memory.Span));

            // We grow far past the buffer so it moves.
            for (int i = 0; i < 2000; i++)
            {
                column.Add(new StringValue($"padding string number {i} with some extra length"), GlobalMemoryManager.Instance);
            }

            Assert.Equal("hello world", Encoding.UTF8.GetString(memory.Span));
            column.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void BinaryValueSurvivesColumnRealloc()
        {
            var column = new BinaryColumn(GlobalMemoryManager.Instance);
            var expected = new byte[] { 1, 2, 3, 4, 5 };
            column.Add(new BinaryValue(expected), GlobalMemoryManager.Instance);

            var value = column.GetValueAt(0, default);

            for (int i = 0; i < 2000; i++)
            {
                column.Add(new BinaryValue(new byte[] { (byte)i, 42, 42, 42, 42, 42, 42, 42 }), GlobalMemoryManager.Instance);
            }

            Assert.True(value.AsBinary.SequenceEqual(expected));
            column.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void StringValueAfterColumnDisposeFailsFast()
        {
            var column = new StringColumn(GlobalMemoryManager.Instance);
            column.Add(new StringValue("hello world"), GlobalMemoryManager.Instance);

            var memory = column.GetValueAt(0, default).AsString.Memory;
            column.Dispose(GlobalMemoryManager.Instance);

            // A disposed column gives an empty span.
            Assert.ThrowsAny<ArgumentOutOfRangeException>(() => memory.Span.Length);
        }

        [Fact]
        public void EmptyStringValueIsEmpty()
        {
            var column = new StringColumn(GlobalMemoryManager.Instance);
            column.Add(new StringValue(""), GlobalMemoryManager.Instance);

            var value = column.GetValueAt(0, default);
            Assert.Equal(0, value.AsString.Memory.Length);
            Assert.Equal("", value.AsString.ToString());
            column.Dispose(GlobalMemoryManager.Instance);
        }
    }
}
