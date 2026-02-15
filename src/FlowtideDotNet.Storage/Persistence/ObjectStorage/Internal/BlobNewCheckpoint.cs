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

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    /// <summary>
    /// Contains information for a new checkpoint
    /// </summary>
    internal class BlobNewCheckpoint
    {
        private readonly PrimitiveList<long> _upsertPageIds;
        private readonly PrimitiveList<long> _upsertPageFileIds;
        private readonly PrimitiveList<int> _upsertPageOffsets;
        private readonly PrimitiveList<int> _upsertPageSizes;

        private readonly PrimitiveList<long> _deletedPageIds;

        // File information
        // Tracks how many pages are no longer active in the file
        // This is used to determine if a file is no longer active or if it can be
        // Rewritten to remove the inactive pages
        private readonly PrimitiveList<long> _changedFileIds;
        private readonly PrimitiveList<int> _changedFilePageCounts;
        private readonly PrimitiveList<int> _changedFileNonActivePageCounts;
        private readonly PrimitiveList<long> _deletedFileIds;

        public BlobNewCheckpoint(IMemoryAllocator memoryAllocator)
        {
            _upsertPageIds = new PrimitiveList<long>(memoryAllocator);
            _upsertPageFileIds = new PrimitiveList<long>(memoryAllocator);
            _upsertPageOffsets = new PrimitiveList<int>(memoryAllocator);
            _upsertPageSizes = new PrimitiveList<int>(memoryAllocator);
            _deletedPageIds = new PrimitiveList<long>(memoryAllocator);
            _changedFileIds = new PrimitiveList<long>(memoryAllocator);
            _changedFilePageCounts = new PrimitiveList<int>(memoryAllocator);
            _changedFileNonActivePageCounts = new PrimitiveList<int>(memoryAllocator);
            _deletedFileIds = new PrimitiveList<long>(memoryAllocator);
        }

        public void AddDeletedPageId(long pageId)
        {
            _deletedPageIds.Add(pageId);
        }

        public void AddFileInformation(FileInformation fileInformation)
        {
            _changedFileIds.Add(fileInformation.FileId);
            _changedFilePageCounts.Add(fileInformation.PageCount);
            _changedFileNonActivePageCounts.Add(fileInformation.NonActivePageCount);
        }

        public void AddDeletedFileId(long fileId)
        {
            _deletedFileIds.Add(fileId);
        }

        [SkipLocalsInit]
        public unsafe void AddUpsertPages(
            PrimitiveList<long> pageIds, 
            PrimitiveList<long> fileIds,
            PrimitiveList<int> pageOffsets,
            PrimitiveList<int> pageSizes)
        {
            if (pageIds.Count < 128)
            {
                Span<int> indices = stackalloc int[pageIds.Count];
                AddUpsertPages_Internal(indices, pageIds, fileIds, pageOffsets, pageSizes);
            }
            else
            {
                int[] indices = new int[pageIds.Count];
                AddUpsertPages_Internal(indices, pageIds, fileIds, pageOffsets, pageSizes);
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
            PrimitiveList<int> pageOffsets,
            PrimitiveList<int> pageSizes)
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
            _upsertPageSizes.EnsureCapacity(_upsertPageSizes.Count + indices.Length);

            int top = _upsertPageFileIds.Count;

            _upsertPageFileIds.SetLength(_upsertPageFileIds.Count + indices.Length);
            _upsertPageIds.SetLength(_upsertPageIds.Count + indices.Length);
            _upsertPageOffsets.SetLength(_upsertPageOffsets.Count + indices.Length);
            _upsertPageSizes.SetLength(_upsertPageSizes.Count + indices.Length);


            var ids = _upsertPageIds.Span;
            var files = _upsertPageFileIds.Span;
            var offsets = _upsertPageOffsets.Span;
            var sizes = _upsertPageSizes.Span;

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
                    sizes.Slice(position, top - position).CopyTo(sizes.Slice(elementIndex + 1));
                }

                // Insert new element
                _upsertPageIds[elementIndex] = pageIds[index];
                _upsertPageFileIds[elementIndex] = fileIds[index];
                _upsertPageOffsets[elementIndex] = pageOffsets[index];
                _upsertPageSizes[elementIndex] = pageSizes[index];

                // Set the new top value top limit how much is copied
                top = position;
            }
        }
    }
}
