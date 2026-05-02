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
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    public static class IntroSort
    {
        private const int IntrosortSizeThreshold = 16;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Sort<T, TComparer>(Span<T> keys, ref TComparer comparer)
            where TComparer : struct, IComparer<T>
        {
            if (keys.Length < 2) return;

            ref T keysRef = ref MemoryMarshal.GetReference(keys);

            int depthLimit = 2 * BitOperations.Log2((uint)keys.Length);

            SortInternal(ref keysRef, 0, keys.Length - 1, depthLimit, ref comparer);
        }

        private static void SortInternal<T, TComparer>(
            ref T keys, int lo, int hi, int depthLimit, ref TComparer comparer)
            where TComparer : struct, IComparer<T>
        {
            while (hi > lo)
            {
                int partitionSize = hi - lo + 1;

                // Fallback to Insertion Sort for tiny partitions
                if (partitionSize <= IntrosortSizeThreshold)
                {
                    if (partitionSize == 2)
                    {
                        SwapIfGreater(ref keys, lo, hi, ref comparer);
                        return;
                    }
                    if (partitionSize == 3)
                    {
                        SwapIfGreater(ref keys, lo, hi - 1, ref comparer);
                        SwapIfGreater(ref keys, lo, hi, ref comparer);
                        SwapIfGreater(ref keys, hi - 1, hi, ref comparer);
                        return;
                    }

                    InsertionSort(ref keys, lo, hi, ref comparer);
                    return;
                }

                // Fallback to HeapSort if QuickSort goes quadratic
                if (depthLimit == 0)
                {
                    HeapSort(ref keys, lo, hi, ref comparer);
                    return;
                }
                depthLimit--;

                // 3. QuickSort Partition
                int p = PickPivotAndPartition(ref keys, lo, hi, ref comparer);

                // Tail Call Optimization: Recurse on the smaller half, loop the larger half
                if (p - lo <= hi - p - 1)
                {
                    SortInternal(ref keys, lo, p, depthLimit, ref comparer);
                    lo = p + 1;
                }
                else
                {
                    SortInternal(ref keys, p + 1, hi, depthLimit, ref comparer);
                    hi = p;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int PickPivotAndPartition<T, TComparer>(
            ref T keys, int lo, int hi, ref TComparer comparer)
            where TComparer : struct, IComparer<T>
        {
            // Median-of-three pivot selection
            int mid = lo + ((hi - lo) >> 1);
            SwapIfGreater(ref keys, lo, mid, ref comparer);
            SwapIfGreater(ref keys, lo, hi, ref comparer);
            SwapIfGreater(ref keys, mid, hi, ref comparer);

            T pivot = Unsafe.Add(ref keys, mid);

            // Hoare Partition
            int i = lo - 1;
            int j = hi + 1;

            while (true)
            {
                while (comparer.Compare(Unsafe.Add(ref keys, ++i), pivot) < 0) ;
                while (comparer.Compare(Unsafe.Add(ref keys, --j), pivot) > 0) ;

                if (i >= j) return j;

                Swap(ref keys, i, j);
            }
        }

        private static void InsertionSort<T, TComparer>(
            ref T keys, int lo, int hi, ref TComparer comparer)
            where TComparer : struct, IComparer<T>
        {
            for (int i = lo; i < hi; i++)
            {
                int j = i;
                T t = Unsafe.Add(ref keys, i + 1);

                while (j >= lo && comparer.Compare(t, Unsafe.Add(ref keys, j)) < 0)
                {
                    Unsafe.Add(ref keys, j + 1) = Unsafe.Add(ref keys, j);
                    j--;
                }
                Unsafe.Add(ref keys, j + 1) = t;
            }
        }

        private static void HeapSort<T, TComparer>(
            ref T keys, int lo, int hi, ref TComparer comparer)
            where TComparer : struct, IComparer<T>
        {
            int n = hi - lo + 1;
            for (int i = n / 2; i >= 1; i--)
            {
                DownHeap(ref keys, i, n, lo, ref comparer);
            }
            for (int i = n; i > 1; i--)
            {
                Swap(ref keys, lo, lo + i - 1);
                DownHeap(ref keys, 1, i - 1, lo, ref comparer);
            }
        }

        private static void DownHeap<T, TComparer>(
            ref T keys, int i, int n, int lo, ref TComparer comparer)
            where TComparer : struct, IComparer<T>
        {
            T d = Unsafe.Add(ref keys, lo + i - 1);
            int child;
            while (i <= n / 2)
            {
                child = 2 * i;
                if (child < n && comparer.Compare(Unsafe.Add(ref keys, lo + child - 1), Unsafe.Add(ref keys, lo + child)) < 0)
                {
                    child++;
                }
                if (comparer.Compare(d, Unsafe.Add(ref keys, lo + child - 1)) >= 0)
                    break;

                Unsafe.Add(ref keys, lo + i - 1) = Unsafe.Add(ref keys, lo + child - 1);
                i = child;
            }
            Unsafe.Add(ref keys, lo + i - 1) = d;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void Swap<T>(ref T keys, int i, int j)
        {
            T temp = Unsafe.Add(ref keys, i);
            Unsafe.Add(ref keys, i) = Unsafe.Add(ref keys, j);
            Unsafe.Add(ref keys, j) = temp;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SwapIfGreater<T, TComparer>(
            ref T keys, int i, int j, ref TComparer comparer)
            where TComparer : struct, IComparer<T>
        {
            if (i != j && comparer.Compare(Unsafe.Add(ref keys, i), Unsafe.Add(ref keys, j)) > 0)
            {
                Swap(ref keys, i, j);
            }
        }
    }
}
