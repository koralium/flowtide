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
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.IO.Hashing;
using System.IO.Pipelines;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal class CheckpointRegistryFile : PipeReader, IEnumerable<CheckpointVersion>, IDisposable, IFileWithSequence
    {
        private const int HeaderSize = 64;
        private const int FooterSize = 8;

        private PrimitiveList<long> _versions;
        private BitmapList _isSnapshots;
        private BitmapList _isBundle;
        private PrimitiveList<ulong> _crc64s;
        private ulong _crc64value;
        private BufferSegment _footerSegment;

        // Read specific fields
        private SequencePosition _advancedPosition;
        private BufferSegment _headerData;
        private BufferSegment _head;
        private BufferSegment _end;
        private int endIndex;
        private bool disposedValue;

        public PrimitiveList<ulong> Crc64s => _crc64s;
        public BufferSegment Head => _head;
        public BufferSegment End => _end;
        public int EndIndex => endIndex;

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, endIndex);

        public CheckpointRegistryFile(IMemoryAllocator memoryAllocator)
        {
            _versions = new PrimitiveList<long>(memoryAllocator);
            _isSnapshots = new BitmapList(memoryAllocator);
            _isBundle = new BitmapList(memoryAllocator);
            _crc64s = new PrimitiveList<ulong>(memoryAllocator);

            _headerData = new BufferSegment(new byte[HeaderSize])
            {
                End = HeaderSize
            };
            _head = _headerData;
            _end = _headerData;
            endIndex = HeaderSize;
            _footerSegment = new BufferSegment(new byte[FooterSize])
            {
                End = FooterSize
            };
        }

        public CheckpointRegistryFile(
            BufferSegment headerData,
            PrimitiveList<long> versions, 
            BitmapList isSnapshots,
            BitmapList isBundle,
            PrimitiveList<ulong> crc64s,
            BufferSegment footer)
        {
            _headerData = headerData;
            _head = _headerData;
            _end = _headerData;
            endIndex = HeaderSize;
            _versions = versions;
            _isSnapshots = isSnapshots;
            _isBundle = isBundle;
            _crc64s = crc64s;
            _footerSegment = footer;
        }

        public void AddCheckpointVersion(CheckpointVersion checkpointVersion)
        {
            _versions.Add(checkpointVersion.Version);
            _isSnapshots.Add(checkpointVersion.IsSnapshot);
            _isBundle.Add(checkpointVersion.IsBundled);
            _crc64s.Add(checkpointVersion.Crc64);
        }

        public void AddCheckpointVersions(IEnumerable<CheckpointVersion> checkpointVersions)
        {
            foreach (var checkpointVersion in checkpointVersions)
            {
                AddCheckpointVersion(checkpointVersion);
            }
        }

        public void RemoveCheckpointVersion(CheckpointVersion checkpointVersion)
        {
            var versionsSpan = _versions.Span;
            for (int i = 0; i < versionsSpan.Length; i++)
            {
                if (versionsSpan[i] == checkpointVersion.Version)
                {
                    _versions.RemoveAt(i);
                    _isSnapshots.RemoveAt(i);
                    _isBundle.RemoveAt(i);
                    _crc64s.RemoveAt(i);
                    break;
                }
            }
        }

        public void Clear()
        {
            _versions.Clear();
            _isSnapshots.Clear();
            _isBundle.Clear();
            _crc64s.Clear();
        }

        public void FinishForWriting()
        {
            _end = _headerData;
            _headerData.ClearNext();
            endIndex = HeaderSize;
            _advancedPosition = default;

            var versionsOffset = (int)(_end.RunningIndex + endIndex);
            var versionsSegment = new BufferSegment(_versions.SlicedMemory);
            _end.SetNext(versionsSegment);
            _end = versionsSegment;
            endIndex = versionsSegment.Length;

            var isSnapshotOffset = (int)(_end.RunningIndex + endIndex);
            var isSnapshotSegment = new BufferSegment(_isSnapshots.MemorySlice);
            _end.SetNext(isSnapshotSegment);
            _end = isSnapshotSegment;
            endIndex = isSnapshotSegment.Length;

            var isBundleOffset = (int)(_end.RunningIndex + endIndex);
            var isBundleSegment = new BufferSegment(_isBundle.MemorySlice);
            _end.SetNext(isBundleSegment);
            _end = isBundleSegment;
            endIndex = isBundleSegment.Length;

            var crc64Offset = (int)(_end.RunningIndex + endIndex);
            var crc64Segment = new BufferSegment(_crc64s.SlicedMemory);
            _end.SetNext(crc64Segment);
            _end = crc64Segment;
            endIndex = crc64Segment.Length;

            var footerOffset = (int)(_end.RunningIndex + endIndex);

            var headerData = _headerData.AvailableMemory.Span;

            // Write magic number
            BinaryPrimitives.WriteInt32LittleEndian(headerData, MagicNumbers.CheckpointRegistryMagicNumber);
            headerData = headerData.Slice(4);

            // Write version
            BinaryPrimitives.WriteInt16LittleEndian(headerData, 1);
            // Next 2 bytes are reserved, so we skip 2
            headerData = headerData.Slice(4);

            // Write counts
            BinaryPrimitives.WriteInt32LittleEndian(headerData, _versions.Count);
            headerData = headerData.Slice(4);

            // Write offsets
            BinaryPrimitives.WriteInt32LittleEndian(headerData, versionsOffset);
            headerData = headerData.Slice(4);

            BinaryPrimitives.WriteInt32LittleEndian(headerData, isSnapshotOffset);
            headerData = headerData.Slice(4);

            BinaryPrimitives.WriteInt32LittleEndian(headerData, isBundleOffset);
            headerData = headerData.Slice(4);

            BinaryPrimitives.WriteInt32LittleEndian(headerData, crc64Offset);
            headerData = headerData.Slice(4);

            BinaryPrimitives.WriteInt32LittleEndian(headerData, footerOffset);
            headerData = headerData.Slice(4);

            _end.SetNext(_footerSegment);
            _end = _footerSegment;
            endIndex = _footerSegment.Length;

            RecalculateAndSetCrc64();
        }

        public void RecalculateAndSetCrc64()
        {
            System.IO.Hashing.Crc64 crc64 = new System.IO.Hashing.Crc64();
            foreach (var segment in WrittenData.Slice(0, WrittenData.Length - 8))
            {
                crc64.Append(segment.Span);
            }
            _crc64value = crc64.GetCurrentHashAsUInt64();
            var footerSpan = _footerSegment.AvailableMemory.Span;
            BinaryPrimitives.WriteUInt64LittleEndian(footerSpan, _crc64value);
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
            _advancedPosition = default;
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

        public static async Task<CheckpointRegistryFile> DeserializeFromBundle(PipeReader reader, IMemoryAllocator memoryAllocator, CancellationToken cancellationToken)
        {
            var headerResult = await reader.ReadAtLeastAsync(CheckpointRegistryBundleFile.HeaderSize);
            // Read header to find out where the registry starts in the bundle file
            var headerEnd = ReadBundleHeader(headerResult.Buffer, out long registryStart);
            reader.AdvanceTo(headerEnd);

            int globalOffset = 64;
            ReadResult result;
            while (true)
            {
                result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                if ((globalOffset + result.Buffer.Length) > registryStart)
                {
                    // We have read past the checkpoint registry, we can stop reading
                    // Advance to start of the registry
                    var positionInSequence = registryStart - globalOffset;
                    var pos = result.Buffer.GetPosition(positionInSequence);
                    reader.AdvanceTo(pos);
                    break;
                }
                else if (result.IsCompleted)
                {
                    throw new InvalidOperationException("Reached end of file before reaching checkpoint registry start offset.");
                }
                globalOffset += (int)result.Buffer.Length;
                reader.AdvanceTo(result.Buffer.End);
            }

            // Read all content of the file
            ReadResult readResult;
            do
            {
                readResult = await reader.ReadAsync(cancellationToken);
                reader.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
            } while (!readResult.IsCompleted);
            var registry = Deserialize(readResult.Buffer, memoryAllocator);
            reader.Complete();
            return registry;
        }

        private static SequencePosition ReadBundleHeader(ReadOnlySequence<byte> header, out long registryStart)
        {
            var reader = new SequenceReader<byte>(header);
            long checkpointStart;
            if (!reader.TryReadLittleEndian(out int magicNumber))
            {
                throw new InvalidDataException("Invalid checkpoint bundle file: unable to read magic number.");
            }
            if (magicNumber == MagicNumbers.BundleCheckpointFileMagicNumber)
            {
                reader.Advance(4);
                reader.TryReadLittleEndian(out checkpointStart);
                reader.TryReadLittleEndian(out registryStart);
            }
            else
            {
                throw new InvalidOperationException($"Unexpected magic number {magicNumber} on checkpoint file");
            }
            reader.Advance(40);
            return reader.Position;
        }

        public static async Task<CheckpointRegistryFile> Deserialize(PipeReader reader, IMemoryAllocator memoryAllocator, CancellationToken cancellationToken)
        {
            // Read all content of the file
            ReadResult readResult;
            do
            {
                readResult = await reader.ReadAsync(cancellationToken);
                reader.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
            } while (!readResult.IsCompleted);
            var registry = Deserialize(readResult.Buffer, memoryAllocator);
            reader.Complete();
            return registry;
        }

        public static CheckpointRegistryFile Deserialize(ReadOnlySequence<byte> fileData, IMemoryAllocator memoryAllocator)
        {
            SequenceReader<byte> reader = new SequenceReader<byte>(fileData);

            // Copy the header bytes to use as the buffersegment for header
            byte[] headerBytes = new byte[HeaderSize];
            if (!reader.TryCopyTo(headerBytes))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read header.");
            }
            BufferSegment headerSegment = new BufferSegment(headerBytes);

            if (!reader.TryReadLittleEndian(out int magicNumber))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read magic number.");
            }
            if (magicNumber != MagicNumbers.CheckpointRegistryMagicNumber)
            {
                throw new InvalidDataException("Invalid checkpoint registry file: incorrect magic number.");
            }
            if (!reader.TryReadLittleEndian(out short version))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read version.");
            }
            if (version != 1)
            {
                throw new InvalidDataException($"Unsupported checkpoint registry file version: {version}.");
            }
            reader.Advance(2); // Skip reserved bytes
            if (!reader.TryReadLittleEndian(out int checkpointCount))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read checkpoint count.");
            }
            if (!reader.TryReadLittleEndian(out int versionsOffset) ||
                !reader.TryReadLittleEndian(out int isSnapshotsOffset) ||
                !reader.TryReadLittleEndian(out int isBundleOffset) ||
                !reader.TryReadLittleEndian(out int crc64sOffset) ||
                !reader.TryReadLittleEndian(out int footerOffset))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read offsets.");
            }

            // Skip reserved bytes
            reader.Advance(32);

            // Verify CRC64
            var crc64 = new Crc64();
            var end = footerOffset;
            foreach (var segment in fileData.Slice(0, end))
            {
                crc64.Append(segment.Span);
            }
            var computedCrc64 = crc64.GetCurrentHashAsUInt64();
            var footerReader = new SequenceReader<byte>(fileData.Slice(footerOffset, FooterSize));
            if (!footerReader.TryReadLittleEndian(out long expectedCrc64Long))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read CRC64 from footer.");
            }
            ulong expectedCrc64 = (ulong)expectedCrc64Long;

            if (computedCrc64 != expectedCrc64)
            {
                throw new InvalidDataException("Checkpoint registry file is corrupted: CRC64 mismatch.");
            }

            var footerSegment = new BufferSegment(fileData.Slice(footerOffset, FooterSize).ToArray());

            var checkpointsVersionMemory = memoryAllocator.Allocate(checkpointCount * sizeof(long), 64);
            reader.Advance(versionsOffset - reader.Consumed);
            if (!reader.TryCopyTo(checkpointsVersionMemory.Memory.Span))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read checkpoint versions.");
            }
            var checkpointVersions = new PrimitiveList<long>(checkpointsVersionMemory, checkpointCount, memoryAllocator);
            reader.Advance(checkpointsVersionMemory.Memory.Length);

            var snapshotMemoryCount = (checkpointCount + 31) / 32 * sizeof(int); // BitmapList uses int for storage, so we calculate the byte size needed for the bitmap
            var isSnapshotsMemory = memoryAllocator.Allocate(snapshotMemoryCount, 64);
            reader.Advance(isSnapshotsOffset - reader.Consumed);
            if (!reader.TryCopyTo(isSnapshotsMemory.Memory.Span))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read snapshot flags.");
            }
            var isSnapshots = new BitmapList(isSnapshotsMemory, checkpointCount, memoryAllocator);
            reader.Advance(isSnapshotsMemory.Memory.Length);

            var bundleMemoryCount = (checkpointCount + 31) / 32 * sizeof(int); // BitmapList uses int for storage, so we calculate the byte size needed for the bitmap
            var isBundleMemory = memoryAllocator.Allocate(bundleMemoryCount, 64);
            reader.Advance(isBundleOffset - reader.Consumed);
            if (!reader.TryCopyTo(isBundleMemory.Memory.Span))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read bundle flags.");
            }
            var isBundles = new BitmapList(isBundleMemory, checkpointCount, memoryAllocator);
            reader.Advance(isBundleMemory.Memory.Length);

            var crc64sMemory = memoryAllocator.Allocate(checkpointCount * sizeof(ulong), 64);
            reader.Advance(crc64sOffset - reader.Consumed);
            if (!reader.TryCopyTo(crc64sMemory.Memory.Span))
            {
                throw new InvalidDataException("Invalid checkpoint registry file: unable to read CRC64 values.");
            }
            var crc64s = new PrimitiveList<ulong>(crc64sMemory, checkpointCount, memoryAllocator);
            reader.Advance(crc64sMemory.Memory.Length);

            return new CheckpointRegistryFile(headerSegment, checkpointVersions, isSnapshots, isBundles, crc64s, footerSegment);
        }

        public IEnumerator<CheckpointVersion> GetEnumerator()
        {
            return GetCheckpointVersions().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private IEnumerable<CheckpointVersion> GetCheckpointVersions()
        {
            for (int i = 0; i < _versions.Count; i++)
            {
                yield return new CheckpointVersion(_versions[i], _isSnapshots[i], _crc64s[i], _isBundle[i]);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                var segment = _head;
                while (segment != null)
                {
                    var next = segment._next;
                    segment.Dispose();
                    segment = next;
                }

                _footerSegment.Dispose();
                _versions.Dispose();
                _isSnapshots.Dispose();
                _isBundle.Dispose();
                _crc64s.Dispose();

                disposedValue = true;
            }
        }

        ~CheckpointRegistryFile()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
