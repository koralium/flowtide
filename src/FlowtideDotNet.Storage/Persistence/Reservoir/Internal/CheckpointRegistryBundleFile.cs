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
using System.Buffers.Binary;
using System.IO.Pipelines;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    /// <summary>
    /// File that bundles a checkpoint file and a checkpoint registry file together. 
    /// This is used to reduce the number of writes and also to simplify registry writing since
    /// it can easily be corrupt if overwritten at the wrong time.
    /// </summary>
    internal class CheckpointRegistryBundleFile : PipeReader
    {
        public const int HeaderSize = 64;

        private readonly BlobNewCheckpoint checkpoint;
        private readonly CheckpointRegistryFile registryFile;
        private SequencePosition _advancedPosition;
        private BufferSegment _headerData;
        private BufferSegment _head;
        private BufferSegment _end;
        private int _endIndex;

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, _endIndex);

        public CheckpointRegistryBundleFile(BlobNewCheckpoint checkpoint, CheckpointRegistryFile registryFile)
        {
            this.checkpoint = checkpoint;
            this.registryFile = registryFile;
            _headerData = new BufferSegment(new byte[HeaderSize])
            {
                End = HeaderSize
            };
            _head = _headerData;
            _end = _headerData;
            _endIndex = HeaderSize;

            var checkpointStartOffset = _end.RunningIndex + _endIndex;
            var segment = checkpoint.Head;
            // Add all the checkpoint data to the end of the file
            while (segment != null)
            {
                var clone = segment.CloneWithoutNextNoOwnership();
                _end.SetNext(clone);
                _end = clone;
                segment = segment._next;
                _endIndex = clone.End;
            }

            var registryStartOffset = _end.RunningIndex + _endIndex;
            var registrySegment = registryFile.Head;
            // Add all the registry data to the end of the file
            while (registrySegment != null)
            {
                var clone = registrySegment.CloneWithoutNextNoOwnership();
                _end.SetNext(clone);
                _end = clone;
                registrySegment = registrySegment._next;
                _endIndex = clone.End;
            }

            var header = _headerData.AvailableMemory.Span;

            BinaryPrimitives.WriteInt32LittleEndian(header, MagicNumbers.BundleCheckpointFileMagicNumber);
            header = header.Slice(8); // 4 bytes are reserved

            BinaryPrimitives.WriteInt64LittleEndian(header, checkpointStartOffset);
            header = header.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(header, registryStartOffset);
            header = header.Slice(8);
            // Rest of the header is reserved
        }

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
        }

        public override void Complete(Exception? exception = null)
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
