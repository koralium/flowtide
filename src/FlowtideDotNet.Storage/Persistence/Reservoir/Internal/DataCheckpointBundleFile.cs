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
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    /// <summary>
    /// A file that contains both data and checkpoint information.
    /// This is used to reduce the number of writes to the storage provider since many cloud providers
    /// cost per write.
    /// </summary>
    internal class DataCheckpointBundleFile : PagesFile
    {
        private readonly MergedBlobFileWriter mergedFile;
        private readonly BlobNewCheckpoint newCheckpoint;
        private readonly CheckpointRegistryFile registry;
        private SequencePosition _advancedPosition;
        private BufferSegment _head;
        private BufferSegment _end;
        private int _endIndex;

        private bool disposedValue;

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, _endIndex);

        public DataCheckpointBundleFile(MergedBlobFileWriter mergedFile, BlobNewCheckpoint newCheckpoint, CheckpointRegistryFile registry)
        {
            this.mergedFile = mergedFile;
            _head = mergedFile.Head;
            _end = mergedFile.End;
            _endIndex = mergedFile.EndIndex;
            this.newCheckpoint = newCheckpoint;
            this.registry = registry;
            var segment = newCheckpoint.Head;

            var checkpointStartOffset = _end.RunningIndex + _endIndex;
            // Add all the checkpoint data to the end of the file
            while (segment != null)
            {
                var clone = segment.CloneWithoutNextNoOwnership();
                _end.SetNext(clone);
                _end = clone;
                if (segment._next != null)
                {
                    segment = segment._next;

                }
                else
                {
                    break;
                }
            }
            _endIndex = newCheckpoint.EndIndex;

            var registryStartOffset = _end.RunningIndex + _endIndex;
            // Add all the registry data to the end of the file
            segment = registry.Head;
            while (segment != null)
            {
                var clone = segment.CloneWithoutNextNoOwnership();
                _end.SetNext(clone);
                _end = clone;
                if (segment._next != null)
                {
                    segment = segment._next;

                }
                else
                {
                    break;
                }
            }
            _endIndex = registry.EndIndex;

            var registryFooterOffset = _end.RunningIndex + _endIndex - 8;

            // Update header info
            var headerSegment = mergedFile.HeaderData;
            var mem = headerSegment.AvailableMemory;
            var headerSpan = mem.Span;

            // Skip to the flags
            headerSpan = headerSpan.Slice(6);
            // Fetch flags and update the first bit to mark that this is a bundle
            var flags = BinaryPrimitives.ReadInt16LittleEndian(headerSpan);
            BinaryPrimitives.WriteInt16LittleEndian(headerSpan, (short)(flags | 1));

            // Skip existing offsets
            headerSpan = headerSpan.Slice(2 + 16);
            // Write new offsets, the checkpoint data starts at the end of the merged file data
            BinaryPrimitives.WriteInt64LittleEndian(headerSpan, checkpointStartOffset);
            headerSpan = headerSpan.Slice(8);

            // Registry offset
            BinaryPrimitives.WriteInt64LittleEndian(headerSpan, registryStartOffset);
            headerSpan = headerSpan.Slice(8);

            // Registry footer offset
            BinaryPrimitives.WriteInt64LittleEndian(headerSpan, registryFooterOffset);
            headerSpan = headerSpan.Slice(8);

            // Recalculate crc64 for the page data, this is needed since we have changed the header
            mergedFile.RecalculateCrc64();
            var newcrc64 = mergedFile.Crc64;

            // The merged file is always the last file
            newCheckpoint.ChangedFileCrc64[newCheckpoint.ChangedFileCrc64.Count - 1] = newcrc64; // Update checkpoint with new crc64 for the changed file
            newCheckpoint.RecalculateCrc64(); // Recalculate crc64 for checkpoint data

            registry.Crc64s[registry.Crc64s.Count - 1] = newCheckpoint.Crc64; // Update registry with new checkpoint crc64
            // Recalculate and set the crc64 of the registry
            registry.RecalculateAndSetCrc64();
        }

        public override PrimitiveList<long> PageIds => mergedFile.PageIds;

        public override PrimitiveList<int> PageOffsets => mergedFile.PageOffsets;

        public override PrimitiveList<uint> Crc32s => mergedFile.Crc32s;

        public override int FileSize => mergedFile.FileSize;

        public override ulong Crc64 => mergedFile.Crc64;

        public override void AdvanceTo(SequencePosition consumed)
        {
            var obj = consumed.GetObject();

            if (obj is byte[] byteArr && byteArr.Length == 0)
            {
                // Nothing was read, so we don't advance
                return;
            }

            _advancedPosition = consumed;
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            _advancedPosition = consumed;
        }

        public override void CancelPendingRead()
        {
            _advancedPosition = default;
        }

        public override void Complete(Exception? exception = null)
        {
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    mergedFile.Return();
                    newCheckpoint.Dispose();
                }

                disposedValue = true;
            }
        }


        public override void Return()
        {
            Dispose(true);
        }

        public override void DoneWriting()
        {
        }

        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            TryRead(out var result);
            return ValueTask.FromResult(result);
        }

        public override bool TryRead(out ReadResult result)
        {
            var data = WrittenData;
            if (data.End.Equals(_advancedPosition))
            {
                result = new ReadResult(ReadOnlySequence<byte>.Empty, false, true);
                return false;
            }
            result = new ReadResult(data.Slice(_advancedPosition), false, true);
            return true;
        }
    }
}
