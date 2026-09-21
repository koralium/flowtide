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

using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Hash
{
    /// <summary>
    /// Class for hashing a batch of rows in a columnar data structure using XxHash32.
    /// This chooses between single-key and multi-key hashing based on the number of fields specified in the constructor.
    /// Single key is a much faster path since it requires no real state machine.
    /// </summary>
    public class BatchHasher
    {
        private readonly int[] _fieldIndices;
        private readonly ReferenceSegment?[] _referenceSegments;
        private int[] _indices = Array.Empty<int>();
        private int[] _scratch = Array.Empty<int>();
        private uint[] _destination = Array.Empty<uint>();
        private Xxh32RowState[] _states = Array.Empty<Xxh32RowState>();

        public BatchHasher(int[] fieldIndices)
        {
            if (fieldIndices.Length == 0)
            {
                throw new ArgumentException("fieldIndices cannot be empty.", nameof(fieldIndices));
            }
            _fieldIndices = fieldIndices;
            _referenceSegments = new ReferenceSegment[fieldIndices.Length];
        }

        public BatchHasher(IReadOnlyList<FieldReference> fieldReferences)
        {
            if (fieldReferences.Count == 0)
            {
                throw new ArgumentException("fieldReferences cannot be empty.", nameof(fieldReferences));
            }
            _fieldIndices = new int[fieldReferences.Count];
            _referenceSegments = new ReferenceSegment[fieldReferences.Count];
            for (int i = 0; i < fieldReferences.Count; i++)
            {
                var fieldReference = fieldReferences[i];
                if (fieldReference is DirectFieldReference directFieldReference && 
                    directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    _fieldIndices[i] = structReferenceSegment.Field;
                    _referenceSegments[i] = structReferenceSegment.Child;
                }
                else
                {
                    throw new NotSupportedException($"Unsupported field reference type: {fieldReference.GetType().Name}");
                }
            }
        }

        public void ResetTemporaryAllocations()
        {
            _indices = Array.Empty<int>();
            _scratch = Array.Empty<int>();
            _destination = Array.Empty<uint>();
            _states = Array.Empty<Xxh32RowState>();
        }

        private void EnsureCapacity(int requiredCount, bool isMultiKey)
        {
            if (_indices.Length < requiredCount)
            {
                int newCapacity = Math.Max(requiredCount, Math.Max(64, _indices.Length * 2));

                _indices = new int[newCapacity];
                _scratch = new int[newCapacity];
                _destination = new uint[newCapacity];

                if (isMultiKey)
                {
                    _states = new Xxh32RowState[newCapacity];
                }

                for (int i = 0; i < newCapacity; i++)
                {
                    _indices[i] = i;
                }
            }
        }

        public ReadOnlySpan<uint> HashBatch(EventBatchData eventBatch)
        {
            var batchCount = eventBatch.Count;
            if (batchCount == 0)
            {
                return ReadOnlySpan<uint>.Empty;
            }

            EnsureCapacity(batchCount, isMultiKey: _fieldIndices.Length > 1);

            var indicesSpan = _indices.AsSpan(0, batchCount);
            var scratchSpan = _scratch.AsSpan(0, batchCount);
            var destSpan = _destination.AsSpan(0, batchCount);

            if (_fieldIndices.Length == 1)
            {
                var column = eventBatch.Columns[_fieldIndices[0]];
                column.ToXxHash32(indicesSpan, _referenceSegments[0], destSpan, scratchSpan);
                return destSpan;
            }

            // Multi-key path
            var statesSpan = _states.AsSpan(0, batchCount);
            for (int i = 0; i < batchCount; i++)
            {
                statesSpan[i].Init();
            }

            for (int c = 0; c < _fieldIndices.Length; c++)
            {
                var col = eventBatch.Columns[_fieldIndices[c]];
                col.AppendToXxHash32(indicesSpan, _referenceSegments[c], statesSpan, scratchSpan);
            }

            for (int i = 0; i < batchCount; i++)
            {
                destSpan[i] = XxHash32Implementation.GetCurrentHashAsUInt32(ref statesSpan[i]);
            }

            return destSpan;
        }
    }
}
