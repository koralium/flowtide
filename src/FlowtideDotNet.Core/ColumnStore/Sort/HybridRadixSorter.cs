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
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    public static class HybridRadixSorter
    {
        [SkipLocalsInit]
        public static void SortBatch(Span<RadixItem> data, int bytePasses)
        {
            if (data.Length <= 1 || bytePasses == 0) return;

            RadixItem[] destArray = ArrayPool<RadixItem>.Shared.Rent(data.Length);
            Span<RadixItem> dest = destArray.AsSpan(0, data.Length);

            Span<RadixItem> currentSource = data;
            Span<RadixItem> currentDest = dest;

            Span<int> histograms = stackalloc int[3072];
            histograms.Clear();

            ref RadixItem sourceRef = ref MemoryMarshal.GetReference(currentSource);

            for (int i = 0; i < data.Length; i++)
            {
                ref RadixItem item = ref Unsafe.Add(ref sourceRef, i);

                for (int pass = 0; pass < bytePasses; pass++)
                {
                    int byteVal = Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), pass);
                    histograms[(pass << 8) + byteVal]++;
                }
            }

            ref RadixItem destRef = ref MemoryMarshal.GetReference(currentDest);

            for (int pass = 0; pass < bytePasses; pass++)
            {
                int histogramOffset = pass << 8;
                int offset = 0;
                Span<int> passHistogram = histograms.Slice(histogramOffset, 256);

                for (int i = 0; i < 256; i++)
                {
                    int count = passHistogram[i];
                    passHistogram[i] = offset;
                    offset += count;
                }

                for (int i = 0; i < currentSource.Length; i++)
                {
                    ref RadixItem item = ref Unsafe.Add(ref sourceRef, i);

                    int byteVal = Unsafe.Add(ref Unsafe.As<RadixItem, byte>(ref item), pass);
                    int destIndex = passHistogram[byteVal]++;

                    Unsafe.Add(ref destRef, destIndex) = item;
                }

                Span<RadixItem> temp = currentSource;
                currentSource = currentDest;
                currentDest = temp;

                ref RadixItem tempRef = ref sourceRef;
                sourceRef = ref destRef;
                destRef = ref tempRef;
            }

            if (bytePasses % 2 != 0)
            {
                currentSource.CopyTo(data);
            }

            ArrayPool<RadixItem>.Shared.Return(destArray);
        }
    }
}
