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

using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using System.Buffers;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Other tests could take this thread's pooled arrays
    [CollectionDefinition(nameof(AllocationCollection), DisableParallelization = true)]
    public class AllocationCollection
    {
    }

    [Collection(nameof(AllocationCollection))]
    public class ComparerAllocationTests
    {
        [Fact]
        public void WarmStructHashingDoesNotAllocate()
        {
            var type = new StructType(new List<Field>()
            {
                new Field("a", Int64Type.Default, false),
                new Field("b", Int64Type.Default, false)
            });
            using var structs = new StructArray(type, 1, new IArrowArray[]
            {
                new Int64Array.Builder().Append(1).Build(),
                new Int64Array.Builder().Append(2).Build()
            }, ArrowBuffer.Empty, 0);
            var comparer = new ArrowStructComparer(new List<IArrowComparer>()
            {
                new ArrowInt64Comparer(), new ArrowInt64Comparer()
            });
            for (int i = 0; i < 1_000; i++)
            {
                ulong state = 0;
                comparer.TryHash(structs, 0, ref state);
            }
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 1_000; i++)
            {
                ulong state = 0;
                comparer.TryHash(structs, 0, ref state);
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.True(allocated == 0, $"Struct hashing allocated {allocated} bytes over 1000 warmed calls");
        }

        [Theory]
        [InlineData("hash")]
        [InlineData("equality")]
        [InlineData("order")]
        public void WarmMapComparisonsDoNotAllocate(string operation)
        {
            const int entriesPerMap = 32;
            const int iterations = 1_000;
            var type = new MapType(Int64Type.Default, Int64Type.Default);
            var keys = new Int64Array.Builder();
            var values = new Int64Array.Builder();
            for (int i = 0; i < entriesPerMap; i++)
            {
                keys.Append(i);
                values.Append(i * 10);
            }
            for (int i = entriesPerMap - 1; i >= 0; i--)
            {
                keys.Append(i);
                values.Append(i * 10);
            }
            var entries = new StructArray(type.KeyValueType, entriesPerMap * 2,
                new IArrowArray[] { keys.Build(), values.Build() }, ArrowBuffer.Empty, 0);
            using var maps = new MapArray(type, 2,
                new ArrowBuffer.Builder<int>().Append(0).Append(entriesPerMap).Append(entriesPerMap * 2).Build(),
                entries, ArrowBuffer.Empty, 0);
            var comparer = new ArrowMapComparer(new ArrowInt64Comparer(), new ArrowInt64Comparer());
            for (int i = 0; i < iterations; i++)
            {
                Check(operation, comparer, maps);
            }
            // Spare arrays of the comparer's size, other threads share the pool
            var spares = Enumerable.Range(0, 16).Select(_ => ArrayPool<int>.Shared.Rent(2 * entriesPerMap)).ToList();
            foreach (var spare in spares)
            {
                ArrayPool<int>.Shared.Return(spare);
            }

            // A per-call allocation shows in every window, a refill only in some
            var allocated = long.MaxValue;
            for (int window = 0; window < 5 && allocated > 0; window++)
            {
                var before = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < iterations; i++)
                {
                    Check(operation, comparer, maps);
                }
                allocated = Math.Min(allocated, GC.GetAllocatedBytesForCurrentThread() - before);
            }

            Assert.True(allocated == 0, $"{operation} allocated at least {allocated} bytes in each of 5 windows of {iterations} warmed calls");
        }

        [Fact]
        public void WarmComparisonsOfTwoArraysDoNotAllocate()
        {
            var type = new StructType(new List<Field>() { new Field("a", Int64Type.Default, false) });
            using var left = new StructArray(type, 1, new IArrowArray[] { new Int64Array.Builder().Append(1).Build() }, ArrowBuffer.Empty, 0);
            using var right = new StructArray(type, 1, new IArrowArray[] { new Int64Array.Builder().Append(1).Build() }, ArrowBuffer.Empty, 0);
            var comparer = new ArrowStructComparer(new List<IArrowComparer>() { new ArrowInt64Comparer() });
            for (int i = 0; i < 1_000; i++)
            {
                comparer.IsEqual(0, left, 0, right);
            }
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 1_000; i++)
            {
                comparer.IsEqual(0, left, 0, right);
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.True(allocated == 0, $"Comparing two struct arrays allocated {allocated} bytes over 1000 warmed calls");
        }

        private static void Check(string operation, ArrowMapComparer comparer, MapArray maps)
        {
            switch (operation)
            {
                case "hash":
                    ulong state = 0;
                    if (!comparer.TryHash(maps, 0, ref state))
                    {
                        throw new InvalidOperationException("A stored map must be hashable");
                    }
                    break;
                case "equality":
                    if (!comparer.IsEqual(0, maps, 1, maps))
                    {
                        throw new InvalidOperationException("Permuted unique maps must be equal");
                    }
                    break;
                default:
                    if (comparer.CompareCanonical(maps, 0, maps, 1) != 0)
                    {
                        throw new InvalidOperationException("Equal maps must compare as zero");
                    }
                    break;
            }
        }
    }
}
