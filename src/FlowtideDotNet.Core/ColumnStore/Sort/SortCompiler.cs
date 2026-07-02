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
        public delegate void SortDelegate(SortCompareContext context, ref Span<int> indices, ref Span<RadixItem> radixItems);

        public delegate void SortWithTagsDelegate(SortCompareContext context, ref Span<int> indices, ref Span<int> tags, ref Span<RadixItem> radixItems);

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

        public const int AvailableBytesRadix = 12;

        private static SortDelegate Compile(IColumn[] columns)
        {
            int availableBytes = AvailableBytesRadix;
            int quicksortStartIndex = 0;
            bool usedRadix = false;

            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                var capability = columns[i].SupportsRadixSort(availableBytes, false);
                availableBytes -= capability.BytesConsumed;
                if (capability.Support == RadixSupport.Full)
                {
                    quicksortStartIndex = i + 1;
                    usedRadix = true;
                }
                else if (capability.Support == RadixSupport.Partial)
                {
                    // Radix grouped the prefix. Quicksort MUST start here to resolve string ties
                    quicksortStartIndex = i;
                    usedRadix = true;
                    break;
                }
                else
                {
                    // RadixSupport.None. Quicksort starts here.
                    quicksortStartIndex = i;
                    break;
                }
            }

            if (usedRadix)
            {
                int bytePasses = AvailableBytesRadix - availableBytes;
                return CompileWithRadix(columns, quicksortStartIndex, bytePasses);
            }

            var comparerType = ComparerStructCompiler.Compile(columns, 0);
            
            var parameter = Expression.Parameter(typeof(SortCompareContext), "context");
            var radixWorkspaceParam = Expression.Parameter(typeof(Span<RadixItem>).MakeByRefType(), "workspace");
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

            var lambda = Expression.Lambda<SortDelegate>(callSort, parameter, indicesParameter, radixWorkspaceParam);

            return lambda.Compile();
        }

        private static SortDelegate CompileWithRadix(IColumn[] columns, int quicksortStartIndex, int bytePasses)
        {
            var comparerType = ComparerStructCompiler.Compile(columns, quicksortStartIndex);

            var contextParam = Expression.Parameter(typeof(SortCompareContext), "context");
            var radixWorkspaceParam = Expression.Parameter(typeof(Span<RadixItem>).MakeByRefType(), "workspace");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");

            var columnsField = Expression.Field(contextParam, nameof(SortCompareContext.columns));
            var executionSteps = new List<Expression>();

            executionSteps.Add(CallResetRadixItems(radixWorkspaceParam, indicesParameter));

            Expression invokeExtractor = CompileRadixExtractor(columns, radixWorkspaceParam, columnsField);
            executionSteps.Add(invokeExtractor);

            var ctor = comparerType.GetConstructor([typeof(SortCompareContext)]);
            if (ctor == null) throw new InvalidOperationException($"Comparer struct {comparerType.FullName} is missing a constructor.");
            var newComparerExpr = Expression.New(ctor, contextParam);

            var doSortRadixMethod = typeof(SortCompiler).GetMethod(
                nameof(DoSortWithRadix),
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);

            if (doSortRadixMethod == null) throw new InvalidOperationException("Could not find DoSortWithRadix method.");

            var callSortRadix = Expression.Call(
                doSortRadixMethod.MakeGenericMethod(comparerType),
                Expression.Constant(bytePasses),
                Expression.Constant(quicksortStartIndex < columns.Length),
                radixWorkspaceParam,
                indicesParameter,
                newComparerExpr
            );

            executionSteps.Add(callSortRadix);

            var masterBlock = Expression.Block(executionSteps);

            var lambda = Expression.Lambda<SortDelegate>(
                masterBlock,
                contextParam,
                indicesParameter,
                radixWorkspaceParam
            );

            return lambda.Compile();
        }

        public static void DoSort<TComparer>(ref Span<int> indices, TComparer comparer) where TComparer : struct, IComparer<int>
        {
            IntroSort.Sort(indices, ref comparer);
        }

        public static void DoSortWithRadix<TComparer>(int bytePasses, bool tieNeeded, ref Span<RadixItem> radixItems, ref Span<int> indices, TComparer comparer) where TComparer : struct, IComparer<int>
        {
            HybridRadixSorter.SortBatch(radixItems, bytePasses);

            for (int i = 0; i < indices.Length; i++)
            {
                indices[i] = radixItems[i].Index;
            }

            if (tieNeeded)
            {
                int length = indices.Length;
                int start = 0;

                for (int i = 1; i <= length; i++)
                {
                    if (i == length ||
                        radixItems[i].PrimaryKey != radixItems[start].PrimaryKey ||
                        radixItems[i].SecondaryKey != radixItems[start].SecondaryKey)
                    {
                        int tieLength = i - start;
                        if (tieLength > 1)
                        {
                            Span<int> tiedIndices = indices.Slice(start, tieLength);
                            IntroSort.Sort(tiedIndices, ref comparer);
                        }
                        start = i;
                    }
                }
            }
        }

        public static SortWithTagsDelegate GetOrCompileWithTags(UInt128 key, IColumn[] columns)
        {
            return _tagsCache.GetOrAdd(key, static (key, args) => CompileWithTags(args), columns);
        }

        private static SortWithTagsDelegate CompileWithTags(IColumn[] columns)
        {
            int availableBytes = AvailableBytesRadix;
            int quicksortStartIndex = 0;
            bool usedRadix = false;

            // --- STEP 1: CAPABILITY ROUTING ---
            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                var capability = columns[i].SupportsRadixSort(availableBytes, false);
                availableBytes -= capability.BytesConsumed;

                if (capability.Support == RadixSupport.Full)
                {
                    quicksortStartIndex = i + 1;
                    usedRadix = true;
                }
                else if (capability.Support == RadixSupport.Partial)
                {
                    // Radix grouped the prefix. Quicksort MUST start here to resolve string ties
                    quicksortStartIndex = i;
                    usedRadix = true;
                    break;
                }
                else
                {
                    // RadixSupport.None. Quicksort starts here.
                    quicksortStartIndex = i;
                    break;
                }
            }

            if (usedRadix)
            {
                int bytePasses = AvailableBytesRadix - availableBytes;
                return CompileWithTagsRadix(columns, quicksortStartIndex, bytePasses);
            }

            var comparerType = ComparerStructCompiler.Compile(columns, 0);

            var parameter = Expression.Parameter(typeof(SortCompareContext), "context");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");
            var tagsParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "tags");

            var radixWorkspaceParam = Expression.Parameter(typeof(Span<RadixItem>).MakeByRefType(), "workspace");

            var ctor = comparerType.GetConstructor([typeof(SortCompareContext)]);

            if (ctor == null)
            {
                throw new InvalidOperationException($"Comparer struct {comparerType.FullName} does not have the expected constructor.");
            }

            var newExpr = Expression.New(ctor, parameter);

            var doSort = typeof(SortCompiler).GetMethod(
                nameof(DoSortWithTags),
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);

            if (doSort == null)
            {
                throw new InvalidOperationException($"Could not find method {nameof(DoSortWithTags)} on {typeof(SortCompiler).FullName}.");
            }

            var callSort = Expression.Call(doSort.MakeGenericMethod(comparerType), indicesParameter, tagsParameter, newExpr);

            var lambda = Expression.Lambda<SortWithTagsDelegate>(
                callSort,
                parameter,
                indicesParameter,
                tagsParameter,
                radixWorkspaceParam
            );

            return lambda.Compile();
        }

        private static SortWithTagsDelegate CompileWithTagsRadix(IColumn[] columns, int quicksortStartIndex, int bytePasses)
        {
            var comparerType = ComparerStructCompiler.Compile(columns, quicksortStartIndex);

            var contextParam = Expression.Parameter(typeof(SortCompareContext), "context");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");
            var tagsParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "tags");

            var radixWorkspaceParam = Expression.Parameter(typeof(Span<RadixItem>).MakeByRefType(), "workspace");

            var columnsField = Expression.Field(contextParam, nameof(SortCompareContext.columns));
            var executionSteps = new List<Expression>();

            executionSteps.Add(CallResetRadixItems(radixWorkspaceParam, indicesParameter));

            Expression invokeExtractor = CompileRadixExtractor(columns, radixWorkspaceParam, columnsField);
            executionSteps.Add(invokeExtractor);

            var ctor = comparerType.GetConstructor([typeof(SortCompareContext)]);
            if (ctor == null)
            {
                throw new InvalidOperationException($"Comparer struct {comparerType.FullName} does not have the expected constructor.");
            }
            var newComparerExpr = Expression.New(ctor, contextParam);

            var doSortRadixMethod = typeof(SortCompiler).GetMethod(
                nameof(DoSortRadixWithTags),
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);

            if (doSortRadixMethod == null)
            {
                throw new InvalidOperationException($"Could not find method {nameof(DoSortRadixWithTags)}.");
            }

            var callSortRadix = Expression.Call(
                doSortRadixMethod.MakeGenericMethod(comparerType),
                Expression.Constant(bytePasses),
                Expression.Constant(quicksortStartIndex < columns.Length),
                radixWorkspaceParam,
                indicesParameter,
                tagsParameter,
                newComparerExpr
            );

            executionSteps.Add(callSortRadix);

            var masterBlock = Expression.Block(executionSteps);

            var lambda = Expression.Lambda<SortWithTagsDelegate>(
                masterBlock,
                contextParam,
                indicesParameter,
                tagsParameter,
                radixWorkspaceParam
            );

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

        public static void DoSortRadixWithTags<TComparer>(int bytePasses, bool tieNeeded, ref Span<RadixItem> radixItems, ref Span<int> indices, ref Span<int> tags, TComparer comparer) where TComparer : struct, IComparer<int>
        {
            HybridRadixSorter.SortBatch(radixItems, bytePasses);

            for (int i = 0; i < indices.Length; i++)
            {
                indices[i] = radixItems[i].Index;
            }
            int length = indices.Length;
            if (length == 0) return;
            
            int start = 0;
            int currentGroup = 0;

            for (int i = 1; i <= length; i++)
            {
                bool isTieBreak = i == length ||
                    Unsafe.As<RadixItem, ulong>(ref radixItems[i]) != Unsafe.As<RadixItem, ulong>(ref radixItems[start]) ||
                    Unsafe.Add(ref Unsafe.As<RadixItem, uint>(ref radixItems[i]), 2) != Unsafe.Add(ref Unsafe.As<RadixItem, uint>(ref radixItems[start]), 2);

                if (isTieBreak)
                {
                    int tieLength = i - start;

                    if (start == 0)
                    {
                        tags[0] = currentGroup;
                    }
                    else
                    {
                        currentGroup++;
                        tags[start] = currentGroup;
                    }

                    if (tieLength > 1)
                    {
                        if (tieNeeded)
                        {
                            Span<int> tiedIndices = indices.Slice(start, tieLength);
                            IntroSort.Sort(tiedIndices, ref comparer);

                            for (int j = start + 1; j < i; j++)
                            {
                                if (comparer.Compare(indices[j - 1], indices[j]) != 0)
                                {
                                    currentGroup++;
                                }
                                tags[j] = currentGroup;
                            }
                        }
                        else
                        {
                            for (int j = start + 1; j < i; j++)
                            {
                                tags[j] = currentGroup;
                            }
                        }
                    }

                    start = i;
                }
            }
        }

        public delegate void ExtractRadixDelegate(ref Span<RadixItem> workspace, IColumn[] columns);

        private static void ResetRadixItems(ref Span<RadixItem> items, ref Span<int> indices)
        {
            for (int i = 0; i < items.Length; i++)
            {
                items[i] = new RadixItem()
                {
                    SecondaryKey = 0,
                    PrimaryKey = 0,
                    Index = indices[i]
                };
            }
        }

        private static Expression CallResetRadixItems(Expression radixItems, Expression indices)
        {
            var method = typeof(SortCompiler).GetMethod(nameof(ResetRadixItems), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
            if (method == null)
            {
                throw new InvalidOperationException($"Could not find method {nameof(ResetRadixItems)} on {typeof(SortCompiler).FullName}.");
            }
            return Expression.Call(method, radixItems, indices);
        }

        public static Expression CompileRadixExtractor(IColumn[] columns, Expression workspaceExpression, Expression columnsExpression)
        {
            var methodCalls = new List<Expression>();

            var setRadixMethod = typeof(IColumn).GetMethod(nameof(IColumn.SetRadixPrefix), [typeof(Span<RadixItem>), typeof(int)]);

            if (setRadixMethod == null)
            {
                throw new InvalidOperationException($"Could not find method {nameof(IColumn.SetRadixPrefix)} on {typeof(IColumn).FullName}.");
            }

            int availableBytes = AvailableBytesRadix;
            
            // First pass: calculate total bytes used and list bytes used per column
            int totalBytes = 0;
            var listBytesUsed = new List<int>();
            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                var column = columns[i];
                var capability = column.SupportsRadixSort(availableBytes, false);

                if (capability.Support == RadixSupport.Full || capability.Support == RadixSupport.Partial)
                {
                    int bytesUsed = capability.BytesConsumed;
                    listBytesUsed.Add(bytesUsed);
                    availableBytes -= bytesUsed;
                    totalBytes += bytesUsed;
                }
                else
                {
                    break;
                }
            }

            // Second pass: generate expressions with correct reverse byte positions
            int cumulativeBytes = 0;
            for (int i = 0; i < listBytesUsed.Count; i++)
            {
                int bytesUsed = listBytesUsed[i];
                cumulativeBytes += bytesUsed;
                int currentBytePosition = totalBytes - cumulativeBytes;

                var columnInstanceExpr = Expression.ArrayIndex(columnsExpression, Expression.Constant(i));
                var positionExpr = Expression.Constant(currentBytePosition, typeof(int));
                var callExpr = Expression.Call(
                    columnInstanceExpr,
                    setRadixMethod,
                    workspaceExpression,
                    positionExpr);

                methodCalls.Add(callExpr);
            }

            var block = Expression.Block(methodCalls);
            return block;
        }
    }
}
