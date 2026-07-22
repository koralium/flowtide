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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    /// <summary>
    /// Region order for the primitive hybrid boundary search. The JIT monomorphizes each instantiation,
    /// so the ascending search compiles to exactly the code it had before directions existed and the
    /// descending search to its mirrored comparisons, without any runtime branch.
    /// </summary>
    internal interface IBoundaryOrder<T> where T : unmanaged
    {
        /// <summary>
        /// True when <paramref name="value"/> is positioned after <paramref name="target"/> in the
        /// region's sort order.
        /// </summary>
        static abstract bool SortsAfter(T value, T target);
    }

    internal readonly struct AscendingBoundaryOrder<T> : IBoundaryOrder<T>
        where T : unmanaged, IComparisonOperators<T, T, bool>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool SortsAfter(T value, T target) => value > target;
    }

    internal readonly struct DescendingBoundaryOrder<T> : IBoundaryOrder<T>
        where T : unmanaged, IComparisonOperators<T, T, bool>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool SortsAfter(T value, T target) => value < target;
    }

    /// <summary>
    /// Row order semantics for the value based fallback searches, matching the four sort field
    /// comparison implementations used by the tree comparers. Each instantiation is monomorphized, so
    /// the null handling and direction are compiled into the search instead of dispatched per row.
    /// </summary>
    internal interface IDirectedValueCompare
    {
        /// <summary>
        /// Compares a tree value against the probe target in region order. Negative when the tree value
        /// is positioned before the target.
        /// </summary>
        static abstract int Compare(IDataValue treeValue, IDataValue target);
    }

    internal readonly struct AscendingNullsLastValueCompare : IDirectedValueCompare
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(IDataValue treeValue, IDataValue target)
        {
            if (treeValue.IsNull)
            {
                return target.IsNull ? 0 : 1;
            }
            if (target.IsNull)
            {
                return -1;
            }
            return DataValueComparer.CompareTo(treeValue, target);
        }
    }

    internal readonly struct DescendingNullsFirstValueCompare : IDirectedValueCompare
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(IDataValue treeValue, IDataValue target)
        {
            if (treeValue.IsNull)
            {
                return target.IsNull ? 0 : -1;
            }
            if (target.IsNull)
            {
                return 1;
            }
            return DataValueComparer.CompareTo(target, treeValue);
        }
    }

    internal readonly struct DescendingNullsLastValueCompare : IDirectedValueCompare
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare(IDataValue treeValue, IDataValue target)
        {
            if (treeValue.IsNull)
            {
                return target.IsNull ? 0 : 1;
            }
            if (target.IsNull)
            {
                return -1;
            }
            return DataValueComparer.CompareTo(target, treeValue);
        }
    }
}
