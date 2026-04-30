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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    internal static class SortCompiler
    {
        public delegate void SortDelegate(SortCompareContext context, ref Span<int> indices);

        public static SortDelegate Compile(IColumn[] columns)
        {
            var comparerType = ComparerStructCompiler.Compile(columns);
            
            var parameter = Expression.Parameter(typeof(SortCompareContext), "context");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");

            var newExpr = Expression.New(comparerType.GetConstructor([typeof(SortCompareContext)]), parameter);

            var doSort = typeof(SortCompiler).GetMethod(nameof(DoSort), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);

            var callSort = Expression.Call(doSort.MakeGenericMethod(comparerType), indicesParameter, newExpr);

            var lambda = Expression.Lambda<SortDelegate>(callSort, parameter, indicesParameter);

            return lambda.Compile();
        }

        public static void DoSort<TComparer>(ref Span<int> indices, TComparer comparer) where TComparer : struct, IComparer<int>
        {
            IntroSort.Sort(indices, ref comparer);
        }

    }
}
