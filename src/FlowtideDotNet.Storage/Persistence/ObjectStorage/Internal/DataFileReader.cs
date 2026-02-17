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
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal struct DataPageInfo
    {
        public long PageId { get; }
        public int Offset { get; }
        public int Length { get; }
        public DataPageInfo(long pageId, int offset, int length)
        {
            PageId = pageId;
            Offset = offset;
            Length = length;
        }
    }

    internal ref struct DataFileReader
    {
        private int pageCount;
        private int pageIdsOffset;
        private int pageOffsetsOffset;
        private int previousPageOffset;

        private SequenceReader<byte> pageIdsReader;
        private SequenceReader<byte> pageOffsetsReader;

        public DataFileReader(ReadOnlySequence<byte> sequence)
        {
            var reader = new SequenceReader<byte>(sequence);

            if (!reader.TryReadLittleEndian(out short version))
            {
                throw new InvalidOperationException("Failed to read version from data file.");
            }

            if (version != 1)
            {
                throw new InvalidOperationException("Unsupported data file version: " + version);
            }

            reader.Advance(2); // Skip reserved bytes

            if (!reader.TryReadLittleEndian(out pageCount))
            {
                throw new InvalidOperationException("Failed to read page count from data file.");
            }

            if (!reader.TryReadLittleEndian(out pageIdsOffset))
            {
                throw new InvalidOperationException("Failed to read page ids offset from data file.");
            }

            if (!reader.TryReadLittleEndian(out pageOffsetsOffset))
            {
                throw new InvalidOperationException("Failed to read page offsets offset from data file.");
            }

            pageIdsReader = new SequenceReader<byte>(sequence.Slice(pageIdsOffset, pageCount * sizeof(long)));
            pageOffsetsReader = new SequenceReader<byte>(sequence.Slice(pageOffsetsOffset, (pageCount + 1) * sizeof(int)));

            if (!pageOffsetsReader.TryReadLittleEndian(out previousPageOffset))
            {
                throw new InvalidOperationException("Failed to read initial page offset from data file.");
            }
        }

        public bool TryGetNextPageInfo(out DataPageInfo pageInfo)
        {
            if (pageIdsReader.Remaining < sizeof(long) || pageOffsetsReader.Remaining < sizeof(int) * 2)
            {
                pageInfo = default;
                return false;
            }
            if (!pageIdsReader.TryReadLittleEndian(out long pageId))
            {
                pageInfo = default;
                return false;
            }
            if (!pageOffsetsReader.TryReadLittleEndian(out int offset))
            {
                pageInfo = default;
                return false;
            }
            pageInfo = new DataPageInfo(pageId, offset, offset - previousPageOffset);
            previousPageOffset = offset;
            return true;
        }
    }
}
