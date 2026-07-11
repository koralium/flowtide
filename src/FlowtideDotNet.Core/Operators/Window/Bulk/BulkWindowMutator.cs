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

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Applies an incoming batch of rows to the bulk window operator's persistent tree.
    /// Weights are merged; when a row's weight reaches zero the row is deleted and all previously emitted
    /// values are retracted immediately. When a weight decreases, the now excess duplicate outputs are
    /// retracted and their state list entries removed. New values for inserted or updated rows are
    /// computed later during the watermark scan.
    /// </summary>
    internal struct BulkWindowMutator : IRowMutator<ColumnRowReference, BulkWindowValue>
    {
        private readonly BulkWindowEmitter _emitter;
        private readonly int _numberOfFunctions;
        private readonly int _totalStateColumns;
        private readonly IDataValue[] _valueScratch;

        public BulkWindowMutator(BulkWindowEmitter emitter, int numberOfFunctions, int totalStateColumns, IDataValue[] valueScratch)
        {
            _emitter = emitter;
            _numberOfFunctions = numberOfFunctions;
            _totalStateColumns = totalStateColumns;
            _valueScratch = valueScratch;
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
                // Negative weights are stored as well so a later matching insert can cancel them,
                // they count as zero logical rows during scans.
                return GenericWriteOperation.Upsert;
            }

            var newWeight = existingData.weight + incomingData.weight;
            incomingData.weight = newWeight;

            var container = existingData.valueContainer;
            if (container == null)
            {
                // The existing value is a pending insert from the same batch that has not been written to
                // a page yet, it has no stored state lists.
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

            if (storedOutputs > newOutputCount && container._previousValueSent.Get(index))
            {
                for (int dup = storedOutputs - 1; dup >= newOutputCount; dup--)
                {
                    for (int f = 0; f < _numberOfFunctions; f++)
                    {
                        _valueScratch[f] = container._functionStates[f].GetListElementValue(index, dup);
                    }
                    _emitter.AddOutputRow(key.referenceBatch, key.RowIndex, _valueScratch, -1);
                }
            }

            // The now excess state entries are removed even when the row is deleted, since a later
            // insert of the same key in the same batch reinstates the row in place. Without clearing, the
            // reinstated row would keep the already retracted values and the scan would retract them again.
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
    }
}
