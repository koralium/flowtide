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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal struct UpsertPageInfo
    {
        public long pageId;
        public long fileId;
        public int offset;
        public int size;
    }

    /// <summary>
    /// Reads the checkpoint data from a byte sequence. 
    /// The checkpoint data contains information about the upserted pages, deleted pages and changed files in the checkpoint
    /// and deleted files in the checkpoint. This information is used to update the in-memory state of the checkpoint handler 
    /// when a checkpoint is loaded.
    /// </summary>
    internal ref struct CheckpointDataReader
    {
        private long upsertPages;
        private long deletedPagesCount;
        private long changedFilesCount;
        private long deletedFilesCount;

        private long upsertPageIdsOffset;
        private long upsertPageFileIdsOffset;
        private long upsertPageFileOffsetsOffset;
        private long upsertPageSizesOffset;
        private long deletedPageIdsOffset;
        private long updatedFileIdsOffset;
        private long updatedFilePageCountOffset;
        private long updatedFileNonActivePageCountOffset;
        private long deletedFileIdsOffset;

        private long nextFileId;

        private SequenceReader<byte> _upsertPageIdsReader;
        private SequenceReader<byte> _upsertPageFileIdsReader;
        private SequenceReader<byte> _upsertPageOffetsReader;
        private SequenceReader<byte> _upsertPageSizesReader;
        private SequenceReader<byte> _deletedPageIdsReader;

        private SequenceReader<byte> _updatedFileIdsReader;
        private SequenceReader<byte> _updatedFilePageCountReader;
        private SequenceReader<byte> _updatedFileNonActivePageCountReader;
        private SequenceReader<byte> _deletedFileIdsReader;

        public long NextFileId => nextFileId;

        public CheckpointDataReader(ReadOnlySequence<byte> dataSequence)
        {
            var reader = new SequenceReader<byte>(dataSequence);

            if (!reader.TryReadLittleEndian(out short version))
            {
                throw new InvalidOperationException("Could not read version");
            }

            if (version != 1)
            {
                throw new InvalidOperationException("Incorrect version, expected version 1");
            }

            // Skip reserved
            reader.Advance(6);

            if (!reader.TryReadLittleEndian(out upsertPages))
            {
                throw new InvalidOperationException("Could not read upsert page count");
            }

            if (!reader.TryReadLittleEndian(out deletedPagesCount))
            {
                throw new InvalidOperationException("Could not read deleted pages count");
            }

            if (!reader.TryReadLittleEndian(out changedFilesCount))
            {
                throw new InvalidOperationException("Could not read changed files count");
            }

            if (!reader.TryReadLittleEndian(out deletedFilesCount))
            {
                throw new InvalidOperationException("Could not read deleted files count");
            }

            if (!reader.TryReadLittleEndian(out upsertPageIdsOffset))
            {
                throw new InvalidOperationException("Could not read upsert page ids offset");
            }

            if (!reader.TryReadLittleEndian(out upsertPageFileIdsOffset))
            {
                throw new InvalidOperationException("Could not read upsert page file ids offset");
            }

            if (!reader.TryReadLittleEndian(out upsertPageFileOffsetsOffset))
            {
                throw new InvalidOperationException("Could not read upsert page offsets offset");
            }

            if (!reader.TryReadLittleEndian(out upsertPageSizesOffset))
            {
                throw new InvalidOperationException("Could not read upsert page sizes offset");
            }

            if (!reader.TryReadLittleEndian(out deletedPageIdsOffset))
            {
                throw new InvalidOperationException("Could not read deleted page ids offset");
            }

            if (!reader.TryReadLittleEndian(out updatedFileIdsOffset))
            {
                throw new InvalidOperationException("Could not read updated file ids offset");
            }

            if (!reader.TryReadLittleEndian(out updatedFilePageCountOffset))
            {
                throw new InvalidOperationException("Could not read updated file page count offset");
            }

            if (!reader.TryReadLittleEndian(out updatedFileNonActivePageCountOffset))
            {
                throw new InvalidOperationException("Could not read updated file page non active count offset");
            }

            if (!reader.TryReadLittleEndian(out deletedFileIdsOffset))
            {
                throw new InvalidOperationException("Could not read deleted file ids offset");
            }

            if (!reader.TryReadLittleEndian(out nextFileId))
            {
                throw new InvalidOperationException("Could not read next file id");
            }

            _upsertPageIdsReader = new SequenceReader<byte>(dataSequence.Slice(upsertPageIdsOffset, upsertPages * sizeof(long)));
            _upsertPageFileIdsReader = new SequenceReader<byte>(dataSequence.Slice(upsertPageFileIdsOffset, upsertPages * sizeof(long)));
            _upsertPageOffetsReader = new SequenceReader<byte>(dataSequence.Slice(upsertPageFileOffsetsOffset, upsertPages * sizeof(int)));
            _upsertPageSizesReader = new SequenceReader<byte>(dataSequence.Slice(upsertPageSizesOffset, upsertPages * sizeof(int)));
            _deletedPageIdsReader = new SequenceReader<byte>(dataSequence.Slice(deletedPageIdsOffset, deletedPagesCount * sizeof(long)));

            _updatedFileIdsReader = new SequenceReader<byte>(dataSequence.Slice(updatedFileIdsOffset, changedFilesCount * sizeof(long)));
            _updatedFilePageCountReader = new SequenceReader<byte>(dataSequence.Slice(updatedFilePageCountOffset, changedFilesCount * sizeof(int)));
            _updatedFileNonActivePageCountReader = new SequenceReader<byte>(dataSequence.Slice(updatedFileNonActivePageCountOffset, changedFilesCount * sizeof(int)));
            _deletedFileIdsReader = new SequenceReader<byte>(dataSequence.Slice(deletedFileIdsOffset, deletedFilesCount * sizeof(long)));
        }

        public bool TryGetNextUpsertPageInfo(out UpsertPageInfo upsertPageInfo)
        {
            upsertPageInfo = new UpsertPageInfo();
            if (!_upsertPageIdsReader.TryReadLittleEndian(out long pageId))
            {
                return false;
            }
            if (!_upsertPageFileIdsReader.TryReadLittleEndian(out long fileId))
            {
                return false;
            }
            if (!_upsertPageOffetsReader.TryReadLittleEndian(out int offset))
            {
                return false;
            }
            if (!_upsertPageSizesReader.TryReadLittleEndian(out int size))
            {
                return false;
            }

            upsertPageInfo.pageId = pageId;
            upsertPageInfo.fileId = fileId;
            upsertPageInfo.offset = offset;
            upsertPageInfo.size = size;

            return true;
        }

        public bool TryGetNextDeletedPageId(out long pageId)
        {
            pageId = 0;
            if (!_deletedPageIdsReader.TryReadLittleEndian(out long deletedPageId))
            {
                return false;
            }
            pageId = deletedPageId;
            return true;
        }

        public bool TryGetFileInformation([NotNullWhen(true)] out FileInformation? fileInformation)
        {
            fileInformation = null;

            if (!_updatedFileIdsReader.TryReadLittleEndian(out long fileId))
            {
                return false;
            }
            if (!_updatedFilePageCountReader.TryReadLittleEndian(out int pageCount))
            {
                return false;
            }
            if (!_updatedFileNonActivePageCountReader.TryReadLittleEndian(out int nonActivePageCount))
            {
                return false;
            }

            fileInformation = new FileInformation(fileId, pageCount, nonActivePageCount);
            return true;
        }

        public bool TryGetNextDeletedFileId(out long fileId)
        {
            fileId = 0;
            if (!_deletedFileIdsReader.TryReadLittleEndian(out long deletedFileId))
            {
                return false;
            }
            fileId = deletedFileId;
            return true;
        }
    }
}
