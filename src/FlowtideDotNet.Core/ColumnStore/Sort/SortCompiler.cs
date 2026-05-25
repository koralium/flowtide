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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    internal static class SortCompiler
    {
        public delegate void SortDelegate(SortCompareContext context, ref Span<int> indices);

        public delegate void SortWithTagsDelegate(SortCompareContext context, ref Span<int> indices, ref Span<int> tags);

        private static readonly ConcurrentDictionary<UInt128, SortDelegate> _cache = new ConcurrentDictionary<UInt128, SortDelegate>();

        private static readonly ConcurrentDictionary<UInt128, SortWithTagsDelegate> _tagsCache = new ConcurrentDictionary<UInt128, SortWithTagsDelegate>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static UInt128 CreateKey(IColumn[] columns)
        {
            UInt128 key = 0;
            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                CompareColumnStateBuilder.BuildColumnsKey(ref key, columns[i].GetColumnState(), i);
            }
            if (columns.Length > 7)
            {
                CompareColumnStateBuilder.AddHasTailToKey(ref key);
            }
            return key;
        }

        public static SortDelegate GetOrCompile(IColumn[] columns)
        {
            var key = CreateKey(columns);
            return GetOrCompile(key, columns);
        }

        public static SortDelegate GetOrCompile(UInt128 key, IColumn[] columns)
        {
            return _cache.GetOrAdd<IColumn[]>(key, static (key, args) => Compile(args), columns);
        }

        private static SortDelegate Compile(IColumn[] columns)
        {
            var comparerType = ComparerStructCompiler.Compile(columns);
            
            var parameter = Expression.Parameter(typeof(SortCompareContext), "context");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");

            var ctor = comparerType.GetConstructor([typeof(SortCompareContext)]);
            if (ctor == null)
            {
                throw new InvalidOperationException($"Comparer struct {comparerType.FullName} does not have the expected constructor.");
            }
            var newExpr = Expression.New(ctor, parameter);

            var doSort = typeof(SortCompiler).GetMethod(nameof(DoSort), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);

            if (doSort == null)
            {
                throw new InvalidOperationException($"Could not find method {nameof(DoSort)} on {typeof(SortCompiler).FullName}.");
            }

            var callSort = Expression.Call(doSort.MakeGenericMethod(comparerType), indicesParameter, newExpr);

            var lambda = Expression.Lambda<SortDelegate>(callSort, parameter, indicesParameter);

            return lambda.Compile();
        }

        public static void DoSort<TComparer>(ref Span<int> indices, TComparer comparer) where TComparer : struct, IComparer<int>
        {
            IntroSort.Sort(indices, ref comparer);
        }

        public static SortWithTagsDelegate GetOrCompileWithTags(UInt128 key, IColumn[] columns)
        {
            return _tagsCache.GetOrAdd(key, static (key, args) => CompileWithTags(args), columns);
        }

        private static SortWithTagsDelegate CompileWithTags(IColumn[] columns)
        {
            var comparerType = ComparerStructCompiler.Compile(columns);

            var parameter = Expression.Parameter(typeof(SortCompareContext), "context");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");
            var tagsParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "tags");

            var ctor = comparerType.GetConstructor([typeof(SortCompareContext)]);

            if (ctor == null)
            {
                throw new InvalidOperationException($"Comparer struct {comparerType.FullName} does not have the expected constructor.");
            }

            var newExpr = Expression.New(ctor, parameter);

            var doSort = typeof(SortCompiler).GetMethod(nameof(DoSortWithTags), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);

            if (doSort == null)
            {
                throw new InvalidOperationException($"Could not find method {nameof(DoSortWithTags)} on {typeof(SortCompiler).FullName}.");
            }

            var callSort = Expression.Call(doSort.MakeGenericMethod(comparerType), indicesParameter, tagsParameter, newExpr);

            var lambda = Expression.Lambda<SortWithTagsDelegate>(callSort, parameter, indicesParameter, tagsParameter);

            return lambda.Compile();
        }

        public static void DoSortWithTags<TComparer>(ref Span<int> indices, ref Span<int> tags, TComparer comparer) where TComparer : struct, IComparer<int>
        {
            IntroSort.Sort(indices, ref comparer);

            if (indices.Length == 0) return;

            // After sorting indices, we need to find tags
            int currentGroup = 0;
            tags[0] = 0;

            for (int i = 1; i < indices.Length; i++)
            {
                if (comparer.Compare(indices[i - 1], indices[i]) != 0)
                {
                    currentGroup++;
                }
                tags[i] = currentGroup;
            }
        }
    }
}
