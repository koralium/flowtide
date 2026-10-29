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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    // The tested column's range, its nulls, and a bit per surviving entry
    internal readonly record struct CandidateDescriptor(int Column, int Lower, int Upper, bool WithNulls, ulong Survivors);

    // Counts each cell's admitted entries, tests small counts one by one
    internal static class CandidateSelector
    {
        // The survivor mask holds this many entries
        public const int MaxDescribed = 64;

        // File ids in id order, no skip reads the descriptors yet
        public static Task<List<int>> Select(DeltaSinkCatalog catalog, ProbeBatch probes, int threshold)
        {
            return Select(catalog.ScanBounds, probes, threshold);
        }

        // Over any scan of bounds rows, as the projection gives them
        public static async Task<List<int>> Select(Func<BoundsVisitor, ValueTask> scan, ProbeBatch probes, int threshold)
        {
            var selected = new List<int>();
            if (probes.Count == 0)
            {
                return selected;
            }
            await scan((id, row) =>
            {
                if (Admits(row, probes, threshold, out _))
                {
                    selected.Add(id);
                }
            });
            return selected;
        }

        // Whether the row's file may hold an entry, described when its entries were tested
        public static bool Admits(ReadOnlySpan<byte> row, ProbeBatch probes, int threshold, out CandidateDescriptor? descriptor)
        {
            descriptor = null;
            if (probes.Count == 0)
            {
                return false;
            }
            var columns = probes.Layout.Columns;
            if ((row[0] & PruningLayout.HasStatistics) == 0 || columns.Count == 0)
            {
                return true;
            }
            var smallest = -1;
            var smallestCount = int.MaxValue;
            var smallestLower = 0;
            var smallestUpper = 0;
            for (int c = 0; c < columns.Count; c++)
            {
                var cell = row.Slice(columns[c].Offset, PruningCell.CellSize(columns[c].Type));
                var column = probes.Column(c);
                var lower = column.Lower(cell);
                var upper = column.Upper(cell, lower);
                var count = upper - lower + (PruningCell.MayHoldNull(cell) ? column.Nulls.Length : 0) + column.Special.Length;
                if (count == 0)
                {
                    return false;
                }
                if (count < smallestCount)
                {
                    smallest = c;
                    smallestCount = count;
                    smallestLower = lower;
                    smallestUpper = upper;
                }
            }
            if (smallestCount > threshold)
            {
                return true;
            }
            // The sorted range, then the nulls, then the specials
            var tested = probes.Column(smallest);
            var withNulls = PruningCell.MayHoldNull(row.Slice(columns[smallest].Offset, PruningCell.CellSize(columns[smallest].Type)));
            ulong survivors = 0;
            var position = 0;
            var any = false;
            for (int i = smallestLower; i < smallestUpper; i++)
            {
                any |= Test(row, probes, tested.Sorted[i], position++, ref survivors);
            }
            if (withNulls)
            {
                foreach (var entry in tested.Nulls)
                {
                    any |= Test(row, probes, entry, position++, ref survivors);
                }
            }
            foreach (var entry in tested.Special)
            {
                any |= Test(row, probes, entry, position++, ref survivors);
            }
            if (any && smallestCount <= MaxDescribed)
            {
                descriptor = new CandidateDescriptor(smallest, smallestLower, smallestUpper, withNulls, survivors);
            }
            return any;
        }

        private static bool Test(ReadOnlySpan<byte> row, ProbeBatch probes, int entry, int position, ref ulong survivors)
        {
            if (!probes.Matches(row, entry))
            {
                return false;
            }
            if (position < MaxDescribed)
            {
                survivors |= 1UL << position;
            }
            return true;
        }
    }
}
