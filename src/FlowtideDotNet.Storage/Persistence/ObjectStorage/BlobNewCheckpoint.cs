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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    /// <summary>
    /// Contains information for a new checkpoint
    /// </summary>
    internal class BlobNewCheckpoint
    {
        private readonly PrimitiveList<long> _upsertPageIds;
        private readonly PrimitiveList<long> _upsertPageFileIds;
        private readonly PrimitiveList<int> _upsertPageOffsets;

        public BlobNewCheckpoint(IMemoryAllocator memoryAllocator)
        {
            _upsertPageIds = new PrimitiveList<long>(memoryAllocator);
            _upsertPageFileIds = new PrimitiveList<long>(memoryAllocator);
            _upsertPageOffsets = new PrimitiveList<int>(memoryAllocator);
        }

        [SkipLocalsInit]
        public unsafe void AddUpsertPages(
            PrimitiveList<long> pageIds, 
            PrimitiveList<long> fileIds,
            PrimitiveList<int> pageOffsets)
        {
            if (pageIds.Count < 128)
            {
                Span<int> indices = stackalloc int[pageIds.Count];
                AddUpsertPages_Internal(indices, pageIds, fileIds, pageOffsets);
            }
            else
            {
                int[] indices = new int[pageIds.Count];
                AddUpsertPages_Internal(indices, pageIds, fileIds, pageOffsets);
            }
        }

        private readonly struct IndiceComparer : IComparer<int>
        {
            private readonly PrimitiveList<long> pageIds;

            public IndiceComparer(PrimitiveList<long> pageIds)
            {
                this.pageIds = pageIds;
            }
            public int Compare(int x, int y)
            {
                return pageIds[x].CompareTo(pageIds[y]);
            }
        }

        private void AddUpsertPages_Internal(
            Span<int> indices,
            PrimitiveList<long> pageIds,
            PrimitiveList<long> fileIds,
            PrimitiveList<int> pageOffsets)
        {
            // Sort the page ids
            int count = pageIds.Count;
            for (int i = 0; i < count; i++)
            {
                indices[i] = i;
            }
            indices.Sort(new IndiceComparer(pageIds));

            // Ensure capacity
            _upsertPageFileIds.EnsureCapacity(_upsertPageFileIds.Count + indices.Length);
            _upsertPageIds.EnsureCapacity(_upsertPageIds.Count + indices.Length);
            _upsertPageOffsets.EnsureCapacity(_upsertPageOffsets.Count + indices.Length);

            int top = _upsertPageFileIds.Count;

            _upsertPageFileIds.SetLength(_upsertPageFileIds.Count + indices.Length);
            _upsertPageIds.SetLength(_upsertPageIds.Count + indices.Length);
            _upsertPageOffsets.SetLength(_upsertPageOffsets.Count + indices.Length);


            var ids = _upsertPageIds.Span;
            var files = _upsertPageFileIds.Span;
            var offsets = _upsertPageOffsets.Span;

            // Start at the end to minimize the amount of memory copies
            for (int i = indices.Length - 1; i >= 0; i--)
            {
                // Binary search the position
                var index = indices[i];
                var position = ids.Slice(0, top).BinarySearch(pageIds[index]);

                if (position >= 0)
                {
                    throw new InvalidOperationException("Page id already exists in the checkpoint.");
                }
                position = ~position;

                var elementIndex = position + i;
                if (top - position > 0)
                {
                    // move forward the elements above with 'i' count.
                    ids.Slice(position, top - position).CopyTo(ids.Slice(elementIndex + 1));
                    files.Slice(position, top - position).CopyTo(files.Slice(elementIndex + 1));
                    offsets.Slice(position, top - position).CopyTo(offsets.Slice(elementIndex + 1));
                }

                // Insert new element
                _upsertPageIds[elementIndex] = pageIds[index];
                _upsertPageFileIds[elementIndex] = fileIds[index];
                _upsertPageOffsets[elementIndex] = pageOffsets[index];

                // Set the new top value top limit how much is copied
                top = position;
            }
        }
    }
}
