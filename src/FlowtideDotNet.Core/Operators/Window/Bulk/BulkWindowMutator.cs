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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Merges weights into the tree and retracts outputs that lost their duplicate.
    /// New values are computed later during the watermark scan.
    /// </summary>
    internal struct BulkWindowMutator : IRowMutator<ColumnRowReference, BulkWindowValue>
    {
        private readonly BulkWindowEmitter _emitter;
        private readonly int _numberOfFunctions;
        private readonly int _totalStateColumns;
        private readonly IDataValue[] _valueScratch;
        private readonly int[] _emitRequiredFunctions;

        public BulkWindowMutator(BulkWindowEmitter emitter, int numberOfFunctions, int totalStateColumns, IDataValue[] valueScratch, int[] emitRequiredFunctions)
        {
            _emitter = emitter;
            _numberOfFunctions = numberOfFunctions;
            _totalStateColumns = totalStateColumns;
            _valueScratch = valueScratch;
            _emitRequiredFunctions = emitRequiredFunctions;
        }

        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            var batch = keys[0].referenceBatch;
            for (int i = 0; i < batch.Columns.Count; i++)
            {
                batch.Columns[i].GetPrefixSumByteSizes(indices, sizes);
            }

            // weight + previous value sent bit + empty state lists
            const int valueByteEstimate = 8;
            var cumulativeValueBytes = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                cumulativeValueBytes += valueByteEstimate;
                sizes[i] += cumulativeValueBytes;
            }
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in BulkWindowValue existingData, ref BulkWindowValue incomingData, int sortedIndex)
        {
            if (!exists)
            {
                // Negative weights are stored so a later insert can cancel them.
                return GenericWriteOperation.Upsert;
            }

            var newWeight = existingData.weight + incomingData.weight;
            incomingData.weight = newWeight;

            var container = existingData.valueContainer;
            if (container == null)
            {
                // Pending insert from the same batch, no stored state lists yet.
                if (newWeight == 0)
                {
                    return GenericWriteOperation.Delete;
                }
                return GenericWriteOperation.Upsert;
            }

            var index = existingData.index;
            var storedOutputs = container._functionStates.Length > 0
                ? container._functionStates[0].GetListLength(index)
                : 0;
            var newOutputCount = Math.Max(0, newWeight);
            AssertStateListsAligned(container, index, storedOutputs);

            if (storedOutputs > newOutputCount && container._previousValueSent.Get(index))
            {
                for (int dup = storedOutputs - 1; dup >= newOutputCount; dup--)
                {
                    for (int f = 0; f < _numberOfFunctions; f++)
                    {
                        _valueScratch[f] = container._functionStates[f].GetListElementValue(index, dup);
                    }
                    // Suppressed duplicates were never sent, nothing to retract.
                    if (WasEmitted())
                    {
                        _emitter.AddOutputRow(key.referenceBatch, key.RowIndex, _valueScratch, -1);
                    }
                }
            }

            // Cleared even when deleted, a same batch reinsert would reuse these and double retract.
            if (storedOutputs > newOutputCount)
            {
                for (int dup = storedOutputs - 1; dup >= newOutputCount; dup--)
                {
                    for (int s = 0; s < _totalStateColumns; s++)
                    {
                        container._functionStates[s].RemoveListElement(index, dup);
                    }
                }
            }

            if (newWeight == 0)
            {
                container._previousValueSent.Unset(index);
                return GenericWriteOperation.Delete;
            }

            return GenericWriteOperation.Upsert;
        }

        private bool WasEmitted()
        {
            for (int i = 0; i < _emitRequiredFunctions.Length; i++)
            {
                if (_valueScratch[_emitRequiredFunctions[i]].IsNull)
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// All state lists of a row must stay the same length, removal indexes them together.
        /// </summary>
        [Conditional("DEBUG")]
        private void AssertStateListsAligned(BulkWindowValueContainer container, int index, int storedOutputs)
        {
            for (int s = 1; s < _totalStateColumns; s++)
            {
                Debug.Assert(
                    container._functionStates[s].GetListLength(index) == storedOutputs,
                    "State list lengths are out of sync between state columns for the same row");
            }
        }
    }
}
