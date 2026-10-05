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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.Check
{
    /// <summary>
    /// Adds net weights to issues, records active transitions per key row and keeps the check's counts.
    /// </summary>
    internal readonly struct CheckWeightMutator : IRowMutator<ColumnRowReference, int>
    {
        private readonly int[] _transitions;
        private readonly CheckCounts _counts;

        public CheckWeightMutator(int[] transitions, CheckCounts counts)
        {
            _transitions = transitions;
            _counts = counts;
        }

        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            CheckKeySizes.GetSizePrefixSum(keys, indices, sizes);
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in int existingData, ref int incomingData, int sortedIndex)
        {
            var previous = exists ? existingData : 0;
            var updated = previous + incomingData;
            if ((previous > 0) != (updated > 0))
            {
                var transition = updated > 0 ? 1 : -1;
                _transitions[key.RowIndex] = transition;
                _counts.ActiveIssues += transition;
            }
            _counts.FailingRows += Math.Max(updated, 0) - Math.Max(previous, 0);
            if (updated == 0)
            {
                return exists ? GenericWriteOperation.Delete : GenericWriteOperation.None;
            }
            // Negative weights are kept, they are never active
            incomingData = updated;
            return GenericWriteOperation.Upsert;
        }
    }
}
