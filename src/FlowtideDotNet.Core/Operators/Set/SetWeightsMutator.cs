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
using FlowtideDotNet.Core.Operators.Set.Structs;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Tree;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Operators.Set
{
    /// <summary>
    /// Adds the input weights and collects the output rows.
    /// </summary>
    internal readonly struct SetWeightsMutator<TStruct> : IRowMutator<ColumnRowReference, TStruct>
        where TStruct : unmanaged, IInputWeight
    {
        private readonly Func<TStruct, int> _weightCalculator;
        private readonly PrimitiveList<uint> _inputIterations;
        private readonly PrimitiveList<int> _outputOffsets;
        private readonly PrimitiveList<int> _outputWeights;
        private readonly PrimitiveList<uint> _outputIterations;

        public SetWeightsMutator(
            Func<TStruct, int> weightCalculator,
            PrimitiveList<uint> inputIterations,
            PrimitiveList<int> outputOffsets,
            PrimitiveList<int> outputWeights,
            PrimitiveList<uint> outputIterations)
        {
            _weightCalculator = weightCalculator;
            _inputIterations = inputIterations;
            _outputOffsets = outputOffsets;
            _outputWeights = outputWeights;
            _outputIterations = outputIterations;
        }

        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            // All keys come from the same batch
            var columns = keys[0].referenceBatch.GetColumns_Unsafe();
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i].GetPrefixSumByteSizes(indices, sizes);
            }

            var valueByteSize = Unsafe.SizeOf<TStruct>();
            var cumulativeValueBytes = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                cumulativeValueBytes += valueByteSize;
                sizes[i] += cumulativeValueBytes;
            }
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in TStruct existingData, ref TStruct incomingData, int sortedIndex)
        {
            if (exists)
            {
                var previousWeight = _weightCalculator(existingData);
                InputWeightExtensions.Add(ref incomingData, existingData);
                var newWeight = _weightCalculator(incomingData);

                var difference = newWeight - previousWeight;
                if (difference != 0)
                {
                    AddOutputRow(key.RowIndex, difference);
                }

                if (incomingData.IsAllZero())
                {
                    return GenericWriteOperation.Delete;
                }
                return GenericWriteOperation.Upsert;
            }

            var weight = _weightCalculator(incomingData);
            if (weight != 0)
            {
                AddOutputRow(key.RowIndex, weight);
            }
            // Always store, another input can add weight later
            return GenericWriteOperation.Upsert;
        }

        private void AddOutputRow(int rowIndex, int weight)
        {
            _outputWeights.Add(weight);
            _outputOffsets.Add(rowIndex);
            _outputIterations.Add(_inputIterations[rowIndex]);
        }
    }
}
