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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Sort
{
    public class ColumnNativeSortTest
    {

        public delegate int CompareDelegate(SelfComparePointers pointers, int x, int y);

        [Fact]
        public void TestSort()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(123));
            column.Add(new DoubleValue(456));

            SelfComparePointers selfComparePointers = new SelfComparePointers();
            column.SetSelfComparePointers(ref selfComparePointers);

            var p1 = Expression.Parameter(typeof(SelfComparePointers), "pointers");
            var p2 = Expression.Parameter(typeof(int), "x");
            var p3 = Expression.Parameter(typeof(int), "y");
            var expr = column.CreateSelfCompareExpression(p1, p2, p3);
            var lambda = Expression.Lambda<CompareDelegate>(expr, p1, p2, p3);
            var compiled = lambda.Compile();

            var compareResult = compiled(selfComparePointers, 0, 1);
        }

        [Fact]
        public void TestSortColumnWithOffset()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new DoubleValue(123),
                new DoubleValue(456),
                NullValue.Instance
            };

            PrimitiveList<int> offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance)
            {
                2,
                1,
                0,
                -1
            };

            ColumnWithOffset columnWithOffset = new ColumnWithOffset(column, offsets);

            SelfComparePointers selfComparePointers = new SelfComparePointers();
            columnWithOffset.SetSelfComparePointers(ref selfComparePointers);

            var p1 = Expression.Parameter(typeof(SelfComparePointers), "pointers");
            var p2 = Expression.Parameter(typeof(int), "x");
            var p3 = Expression.Parameter(typeof(int), "y");
            var expr = columnWithOffset.CreateSelfCompareExpression(p1, p2, p3);
            var lambda = Expression.Lambda<CompareDelegate>(expr, p1, p2, p3);
            var compiled = lambda.Compile();

            var compareResult = compiled(selfComparePointers, 0, 3);
            Assert.Equal(0, compareResult);
            Assert.True(compiled(selfComparePointers, 1, 2) > 0);
            Assert.True(compiled(selfComparePointers, 3, 2) < 0);
            Assert.True(compiled(selfComparePointers, 1, 0) > 0);
        }

        [Fact]
        public void TestSortColumnWithOffsetNoNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new DoubleValue(123),
                new DoubleValue(456)
            };

            PrimitiveList<int> offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance)
            {
                1,
                0,
                -1
            };

            ColumnWithOffset columnWithOffset = new ColumnWithOffset(column, offsets);

            SelfComparePointers selfComparePointers = new SelfComparePointers();
            columnWithOffset.SetSelfComparePointers(ref selfComparePointers);

            var p1 = Expression.Parameter(typeof(SelfComparePointers), "pointers");
            var p2 = Expression.Parameter(typeof(int), "x");
            var p3 = Expression.Parameter(typeof(int), "y");
            var expr = columnWithOffset.CreateSelfCompareExpression(p1, p2, p3);
            var lambda = Expression.Lambda<CompareDelegate>(expr, p1, p2, p3);
            var compiled = lambda.Compile();

            var compareResult = compiled(selfComparePointers, 0, 2);
            Assert.Equal(1, compareResult);
        }
    }
}
