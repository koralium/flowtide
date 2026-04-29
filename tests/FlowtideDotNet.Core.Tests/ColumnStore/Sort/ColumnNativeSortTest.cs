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
            column.Add(NullValue.Instance);

            SelfComparePointers selfComparePointers = new SelfComparePointers();
            column.SetSelfComparePointers(ref selfComparePointers);

            var p1 = Expression.Parameter(typeof(SelfComparePointers), "pointers");
            var p2 = Expression.Parameter(typeof(int), "x");
            var p3 = Expression.Parameter(typeof(int), "y");
            var expr = column.CreateSelfCompareExpression(p1, p2, p3);
            var lambda = Expression.Lambda<CompareDelegate>(expr, p1, p2, p3);
            var compiled = lambda.Compile();

            var compareResult = compiled(selfComparePointers, 0, 2);
        }

        [Fact]
        public void TestSortColumnWithOffset()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(123));
            column.Add(new DoubleValue(456));
            column.Add(NullValue.Instance);


            ColumnWithOffset columnWithOffset = new ColumnWithOffset(column, [2, 1, 0], false);

            SelfComparePointers selfComparePointers = new SelfComparePointers();
            column.SetSelfComparePointers(ref selfComparePointers);

            var p1 = Expression.Parameter(typeof(SelfComparePointers), "pointers");
            var p2 = Expression.Parameter(typeof(int), "x");
            var p3 = Expression.Parameter(typeof(int), "y");
            var expr = column.CreateSelfCompareExpression(p1, p2, p3);
            var lambda = Expression.Lambda<CompareDelegate>(expr, p1, p2, p3);
            var compiled = lambda.Compile();

            var compareResult = compiled(selfComparePointers, 0, 2);
        }
    }
}
