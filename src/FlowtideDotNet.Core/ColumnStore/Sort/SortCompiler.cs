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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    internal static class SortCompiler
    {
        public delegate void SortDelegate(SortCompareContext context, ref Span<int> indices, ref Span<RadixItem> radixItems);

        public delegate void SortWithTagsDelegate(SortCompareContext context, ref Span<int> indices, ref Span<int> tags, ref Span<RadixItem> radixItems);

        /// <summary>
        /// Global compile cache key. The column state key only covers the fast path columns, so the
        /// column count and the per column directions are part of the key as well: the compiled delegates
        /// bake the tail column count and every direction in, nothing is decided at runtime. Instances of
        /// <see cref="BatchSorter"/> have fixed directions and column count, so their per batch fast path
        /// only needs to compare the state key; this composite key is only touched on a state change.
        /// </summary>
        private readonly struct SortLayoutKey : IEquatable<SortLayoutKey>
        {
            public readonly UInt128 StateKey;
            public readonly int ColumnCount;
            public readonly SortColumnDirection[]? Directions;

            public SortLayoutKey(UInt128 stateKey, int columnCount, SortColumnDirection[]? directions)
            {
                StateKey = stateKey;
                ColumnCount = columnCount;
                Directions = directions;
            }

            public bool Equals(SortLayoutKey other)
            {
                if (StateKey != other.StateKey || ColumnCount != other.ColumnCount)
                {
                    return false;
                }
                if (ReferenceEquals(Directions, other.Directions))
                {
                    return true;
                }
                if (Directions == null || other.Directions == null || Directions.Length != other.Directions.Length)
                {
                    return false;
                }
                for (int i = 0; i < Directions.Length; i++)
                {
                    if (Directions[i] != other.Directions[i])
                    {
                        return false;
                    }
                }
                return true;
            }

            public override bool Equals(object? obj)
            {
                return obj is SortLayoutKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                var hash = HashCode.Combine(StateKey, ColumnCount);
                if (Directions != null)
                {
                    for (int i = 0; i < Directions.Length; i++)
                    {
                        hash = HashCode.Combine(hash, Directions[i]);
                    }
                }
                return hash;
            }
        }

        private static readonly ConcurrentDictionary<SortLayoutKey, SortDelegate> _cache = new ConcurrentDictionary<SortLayoutKey, SortDelegate>();

        private static readonly ConcurrentDictionary<SortLayoutKey, SortWithTagsDelegate> _tagsCache = new ConcurrentDictionary<SortLayoutKey, SortWithTagsDelegate>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static UInt128 CreateKey(IColumn[] columns)
        {
            return CreateKey(columns, null);
        }

        public static UInt128 CreateKey(IColumn[] columns, SortColumnDirection[]? directions)
        {
            UInt128 key = 0;
            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                var state = columns[i].GetColumnState();
                if (directions != null && i < directions.Length)
                {
                    state = CompareColumnStateBuilder.ApplyDirection(state, directions[i]);
                }
                CompareColumnStateBuilder.BuildColumnsKey(ref key, state, i);
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
            return GetOrCompile(key, columns, null);
        }

        public static SortDelegate GetOrCompile(UInt128 key, IColumn[] columns, SortColumnDirection[]? directions)
        {
            return _cache.GetOrAdd(new SortLayoutKey(key, columns.Length, directions), static (key, args) => Compile(args.columns, args.directions), (columns, directions));
        }

        public const int AvailableBytesRadix = 12;

        /// <summary>
        /// One radix absorbed column: which column writes its prefix bytes at which position, and whether
        /// its byte range is complemented afterwards to invert the order for a descending column.
        /// </summary>
        private readonly struct RadixExtraction
        {
            public readonly int ColumnIndex;
            public readonly int BytePosition;

            public RadixExtraction(int columnIndex, int bytePosition)
            {
                ColumnIndex = columnIndex;
                BytePosition = bytePosition;
            }
        }

        /// <summary>
        /// The shared routing decision for a column layout: which columns the radix prefix absorbs, where
        /// the tie comparer starts and the combined complement masks for descending columns. Computed once
        /// so the sort entry points and the extractor can never disagree.
        /// </summary>
        private sealed class RadixPlan
        {
            public int QuicksortStartIndex;
            public int BytePasses;
            public bool UsedRadix;
            public ulong PrimaryComplementMask;
            public uint SecondaryComplementMask;
            public List<RadixExtraction> Extractions = new List<RadixExtraction>();
        }

        private static RadixPlan ComputePlan(IColumn[] columns, SortColumnDirection[]? directions)
        {
            var plan = new RadixPlan();
            int availableBytes = AvailableBytesRadix;
            var consumed = new List<(int columnIndex, int bytes, bool complement)>();

            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                var direction = BatchSortCompiler.GetEffectiveDirection(columns[i], directions, i);
                if (direction.HasSwappedNulls())
                {
                    // The null marker byte is less significant than the value bytes within a column's
                    // range, so null placement opposite the value order cannot be encoded in the prefix.
                    // The comparer resolves the order from this column on.
                    plan.QuicksortStartIndex = i;
                    break;
                }

                var capability = columns[i].SupportsRadixSort(availableBytes, false);
                availableBytes -= capability.BytesConsumed;
                if (capability.Support == RadixSupport.Full)
                {
                    consumed.Add((i, capability.BytesConsumed, direction.IsDescending()));
                    plan.QuicksortStartIndex = i + 1;
                    plan.UsedRadix = true;
                }
                else if (capability.Support == RadixSupport.Partial)
                {
                    // Radix grouped the prefix. Quicksort MUST start here to resolve string ties
                    consumed.Add((i, capability.BytesConsumed, direction.IsDescending()));
                    plan.QuicksortStartIndex = i;
                    plan.UsedRadix = true;
                    break;
                }
                else
                {
                    // RadixSupport.None. Quicksort starts here.
                    plan.QuicksortStartIndex = i;
                    break;
                }
            }

            int totalBytes = 0;
            for (int i = 0; i < consumed.Count; i++)
            {
                totalBytes += consumed[i].bytes;
            }
            plan.BytePasses = totalBytes;

            int cumulativeBytes = 0;
            for (int i = 0; i < consumed.Count; i++)
            {
                cumulativeBytes += consumed[i].bytes;
                int bytePosition = totalBytes - cumulativeBytes;
                plan.Extractions.Add(new RadixExtraction(consumed[i].columnIndex, bytePosition));

                if (consumed[i].complement)
                {
                    // A descending column inverts its whole byte range, including the null marker byte,
                    // which turns nulls-first ascending into nulls-last descending.
                    for (int b = bytePosition; b < bytePosition + consumed[i].bytes; b++)
                    {
                        if (b < 4)
                        {
                            plan.SecondaryComplementMask |= (uint)0xFF << (8 * b);
                        }
                        else
                        {
                            plan.PrimaryComplementMask |= (ulong)0xFF << (8 * (b - 4));
                        }
                    }
                }
            }

            return plan;
        }

        private static SortDelegate Compile(IColumn[] columns, SortColumnDirection[]? directions)
        {
            var plan = ComputePlan(columns, directions);

            if (plan.UsedRadix)
            {
                return CompileWithRadix(columns, directions, plan);
            }

            var comparerType = ComparerStructCompiler.Compile(columns, 0, directions);

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

        private static SortDelegate CompileWithRadix(IColumn[] columns, SortColumnDirection[]? directions, RadixPlan plan)
        {
            var comparerType = ComparerStructCompiler.Compile(columns, plan.QuicksortStartIndex, directions);

            var contextParam = Expression.Parameter(typeof(SortCompareContext), "context");
            var radixWorkspaceParam = Expression.Parameter(typeof(Span<RadixItem>).MakeByRefType(), "workspace");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");

            var columnsField = Expression.Field(contextParam, nameof(SortCompareContext.columns));
            var executionSteps = new List<Expression>();

            executionSteps.Add(CallResetRadixItems(radixWorkspaceParam, indicesParameter));

            AddRadixExtractorSteps(plan, radixWorkspaceParam, columnsField, executionSteps);

            var ctor = comparerType.GetConstructor([typeof(SortCompareContext)]);
            if (ctor == null) throw new InvalidOperationException($"Comparer struct {comparerType.FullName} is missing a constructor.");
            var newComparerExpr = Expression.New(ctor, contextParam);

            var doSortRadixMethod = typeof(SortCompiler).GetMethod(
                nameof(DoSortWithRadix),
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);

            if (doSortRadixMethod == null) throw new InvalidOperationException("Could not find DoSortWithRadix method.");

            var callSortRadix = Expression.Call(
                doSortRadixMethod.MakeGenericMethod(comparerType),
                Expression.Constant(plan.BytePasses),
                Expression.Constant(plan.QuicksortStartIndex < columns.Length),
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
            return GetOrCompileWithTags(key, columns, null);
        }

        public static SortWithTagsDelegate GetOrCompileWithTags(UInt128 key, IColumn[] columns, SortColumnDirection[]? directions)
        {
            return _tagsCache.GetOrAdd(new SortLayoutKey(key, columns.Length, directions), static (key, args) => CompileWithTags(args.columns, args.directions), (columns, directions));
        }

        private static SortWithTagsDelegate CompileWithTags(IColumn[] columns, SortColumnDirection[]? directions)
        {
            var plan = ComputePlan(columns, directions);

            if (plan.UsedRadix)
            {
                return CompileWithTagsRadix(columns, directions, plan);
            }

            var comparerType = ComparerStructCompiler.Compile(columns, 0, directions);

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

        private static SortWithTagsDelegate CompileWithTagsRadix(IColumn[] columns, SortColumnDirection[]? directions, RadixPlan plan)
        {
            var comparerType = ComparerStructCompiler.Compile(columns, plan.QuicksortStartIndex, directions);

            var contextParam = Expression.Parameter(typeof(SortCompareContext), "context");
            var indicesParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "indices");
            var tagsParameter = Expression.Parameter(typeof(Span<int>).MakeByRefType(), "tags");

            var radixWorkspaceParam = Expression.Parameter(typeof(Span<RadixItem>).MakeByRefType(), "workspace");

            var columnsField = Expression.Field(contextParam, nameof(SortCompareContext.columns));
            var executionSteps = new List<Expression>();

            executionSteps.Add(CallResetRadixItems(radixWorkspaceParam, indicesParameter));

            AddRadixExtractorSteps(plan, radixWorkspaceParam, columnsField, executionSteps);

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
                Expression.Constant(plan.BytePasses),
                Expression.Constant(plan.QuicksortStartIndex < columns.Length),
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

        /// <summary>
        /// Complements the byte ranges of every descending column in one pass, inverting their order in
        /// the prefix. Runs once per batch with compile time constant masks, so direction never appears as
        /// a branch in any per row loop.
        /// </summary>
        private static void ApplyComplementMasks(ref Span<RadixItem> items, ulong primaryMask, uint secondaryMask)
        {
            ref RadixItem itemsRef = ref MemoryMarshal.GetReference(items);
            for (int i = 0; i < items.Length; i++)
            {
                ref RadixItem item = ref Unsafe.Add(ref itemsRef, i);
                item.PrimaryKey ^= primaryMask;
                item.SecondaryKey ^= secondaryMask;
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

        private static void AddRadixExtractorSteps(RadixPlan plan, Expression workspaceExpression, Expression columnsExpression, List<Expression> executionSteps)
        {
            var setRadixMethod = typeof(IColumn).GetMethod(nameof(IColumn.SetRadixPrefix), [typeof(Span<RadixItem>), typeof(int)]);

            if (setRadixMethod == null)
            {
                throw new InvalidOperationException($"Could not find method {nameof(IColumn.SetRadixPrefix)} on {typeof(IColumn).FullName}.");
            }

            for (int i = 0; i < plan.Extractions.Count; i++)
            {
                var extraction = plan.Extractions[i];
                var columnInstanceExpr = Expression.ArrayIndex(columnsExpression, Expression.Constant(extraction.ColumnIndex));
                var positionExpr = Expression.Constant(extraction.BytePosition, typeof(int));
                var callExpr = Expression.Call(
                    columnInstanceExpr,
                    setRadixMethod,
                    workspaceExpression,
                    positionExpr);

                executionSteps.Add(callExpr);
            }

            if (plan.PrimaryComplementMask != 0 || plan.SecondaryComplementMask != 0)
            {
                var complementMethod = typeof(SortCompiler).GetMethod(nameof(ApplyComplementMasks), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
                if (complementMethod == null)
                {
                    throw new InvalidOperationException($"Could not find method {nameof(ApplyComplementMasks)} on {typeof(SortCompiler).FullName}.");
                }
                executionSteps.Add(Expression.Call(
                    complementMethod,
                    workspaceExpression,
                    Expression.Constant(plan.PrimaryComplementMask),
                    Expression.Constant(plan.SecondaryComplementMask)));
            }
        }
    }
}
