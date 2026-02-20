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
using System.Drawing;
using System.IO.Hashing;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    /// <summary>
    /// Data file reader specialized to read page data in a streaming fashion.
    /// Used when compacting a data file to quickly copy data into new files.
    /// </summary>
    internal class DataFileReader
    {
        private const int HeaderStep = 0;
        private const int PageIdsStep = 1;
        private const int PageOffsetsStep = 2;
        private const int DataStep = 3;

        private readonly Crc64PipeReader _pipeReader;
        private int pageCount;
        private int pageIdsOffset;
        private int pageOffsetsOffset;
        private int dataStartOffset;

        private int _globalOffset;
        private int step = 0;
        private SequencePosition _consumedEnd;

        public DataFileReader(PipeReader pipeReader)
        {
            this._pipeReader = new Crc64PipeReader(pipeReader);
        }

        public ulong GetCrc64()
        {
            return _pipeReader.GetCrc64();
        }

        public ValueTask Initialize()
        {
            if (step != HeaderStep)
            {
                throw new InvalidOperationException("Initialize must be the first called method");
            }
            step++;
            var result = _pipeReader.ReadAtLeastAsync(64);
            if (result.IsCompletedSuccessfully)
            {
                ReadHeaderResult(result.Result);
                return ValueTask.CompletedTask;
            }
            return Initialize_Slow(result);
        }

        private async ValueTask Initialize_Slow(ValueTask<ReadResult> task)
        {
            var result = await task;
            ReadHeaderResult(result);
        }

        private void ReadHeaderResult(ReadResult readResult)
        {
            var sequenceReader = new SequenceReader<byte>(readResult.Buffer);

            if (!sequenceReader.TryReadLittleEndian(out int magicNumber))
            {
                throw new InvalidOperationException("Failed to read magic number from data file");
            }

            if (magicNumber != MagicNumbers.DataFileMagicNumber)
            {
                throw new InvalidOperationException("Magic number missmatch for data file");
            }

            if (!sequenceReader.TryReadLittleEndian(out short version))
            {
                throw new InvalidOperationException("Failed to read version from data file.");
            }

            if (version != 1)
            {
                throw new InvalidOperationException("Unsupported data file version: " + version);
            }

            sequenceReader.Advance(2); // Skip reserved bytes

            if (!sequenceReader.TryReadLittleEndian(out pageCount))
            {
                throw new InvalidOperationException("Failed to read page count from data file.");
            }

            if (!sequenceReader.TryReadLittleEndian(out pageIdsOffset))
            {
                throw new InvalidOperationException("Failed to read page ids offset from data file.");
            }

            if (!sequenceReader.TryReadLittleEndian(out pageOffsetsOffset))
            {
                throw new InvalidOperationException("Failed to read page offsets offset from data file.");
            }

            if (!sequenceReader.TryReadLittleEndian(out dataStartOffset))
            {
                throw new InvalidOperationException("Failed to read data start offset from data file.");
            }

            // Skip reserved bytes
            sequenceReader.Advance(40);

            // Advance the pipe reader
            
            _pipeReader.AdvanceTo(sequenceReader.Position);
            _globalOffset = 64;
        }

        public async ValueTask<ReadOnlySequence<byte>> ReadPageIds()
        {
            if (step != PageIdsStep)
            {
                throw new InvalidOperationException("Page ids must be fetched after initialize");
            }
            step++;
            var lengthOfIds = pageOffsetsOffset - _globalOffset;
            var pageIdsResult = await _pipeReader.ReadAtLeastAsync(lengthOfIds);
            var slice = pageIdsResult.Buffer.Slice(0, lengthOfIds);
            _consumedEnd = slice.End;
            _globalOffset += (int)slice.Length;
            return slice;
        }

        public async ValueTask<ReadOnlySequence<byte>> ReadPageOffsets()
        {
            if (step != PageOffsetsStep)
            {
                throw new InvalidOperationException("Page offsets must be fetched after page ids");
            }
            step++;
            _pipeReader.AdvanceTo(_consumedEnd);
            var lengthOfOffsets = dataStartOffset - _globalOffset;
            var pageOffsetsResult = await _pipeReader.ReadAtLeastAsync(lengthOfOffsets);
            var slice = pageOffsetsResult.Buffer.Slice(0, lengthOfOffsets);
            _consumedEnd = slice.End;
            _globalOffset += (int)slice.Length;
            return slice;
        }

        public async ValueTask SkipPageOffsets()
        {
            if(step != PageOffsetsStep)
            {
                throw new InvalidOperationException("Page offsets must be fetched after page ids");
            }
            step++;
            _pipeReader.AdvanceTo(_consumedEnd);
            var lengthOfOffsets = dataStartOffset - _globalOffset;
            var toConsume = lengthOfOffsets;

            do
            {
                var result = await _pipeReader.ReadAsync();
                if (result.Buffer.Length < toConsume)
                {
                    toConsume -= (int)result.Buffer.Length;
                    _pipeReader.AdvanceTo(result.Buffer.End);
                }
                else
                {
                    var slice = result.Buffer.Slice(toConsume);
                    _consumedEnd = slice.Start;
                    break;
                }
            } while (true);
            _globalOffset += lengthOfOffsets;
        }

        public async ValueTask<ReadOnlySequence<byte>> ReadDataPage(int offset, int length)
        {
            if (step != DataStep)
            {
                throw new InvalidOperationException("Read data page must be done after fetching page offsets");
            }
            if (offset < _globalOffset)
            {
                throw new InvalidOperationException("Read data page must be done in sequence");
            }
            _pipeReader.AdvanceTo(_consumedEnd);

            var difference = offset - _globalOffset;
            // Loop through the pipe to find the next full data page
            ReadResult readResult;
            do
            {
                readResult = await _pipeReader.ReadAsync();
                if (readResult.Buffer.Length < difference)
                {
                    // If the difference is greater than the buffer, skip the entire buffer
                    _pipeReader.AdvanceTo(readResult.Buffer.End);
                    difference = (int)(difference - readResult.Buffer.Length);
                }
                else
                {
                    var slice = readResult.Buffer.Slice(difference);
                    if (slice.Length < length)
                    {
                        difference = 0;
                        _pipeReader.AdvanceTo(slice.Start, slice.End);
                    }
                    else
                    {
                        break;
                    }
                }
            } while (true);

            var dataSlice = readResult.Buffer.Slice(difference, length);
            _consumedEnd = dataSlice.End;
            _globalOffset = offset + length;
            _consumedEnd = dataSlice.End;
            return dataSlice;
        }

        public async ValueTask ReadToEnd(CancellationToken cancellationToken = default)
        {
            _pipeReader.AdvanceTo(_consumedEnd);


            ReadResult readResult;
            do
            {
                readResult = await _pipeReader.ReadAsync(cancellationToken).ConfigureAwait(false);
                _pipeReader.AdvanceTo(readResult.Buffer.End);
            } while (!readResult.IsCompleted);
        }
    }
}
