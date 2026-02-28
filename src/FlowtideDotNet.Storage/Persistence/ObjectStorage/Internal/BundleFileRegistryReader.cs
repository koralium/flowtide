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

using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    /// <summary>
    /// This class contains the code to read out the registry from a bundled file
    /// </summary>
    internal class BundleFileRegistryReader
    {
        private readonly PipeReader _pipeReader;
        private long _checkpointStartOffset;
        private long _checkpointRegistryStartOffset;
        private long _registryFooterOffset;
        private int _globalOffset;

        private BundleFileRegistryReader(PipeReader pipeReader) 
        {
            _pipeReader = pipeReader;
        }

        public static ValueTask<ReadOnlySequence<byte>> ReadCheckpointDataAsync(PipeReader pipeReader, CancellationToken cancellationToken)
        {
            var reader = new BundleFileRegistryReader(pipeReader);
            return reader.GetCheckpointData(cancellationToken);
        }
            
        private async ValueTask<ReadOnlySequence<byte>> GetCheckpointData(CancellationToken cancellationToken)
        {
            var result = await _pipeReader.ReadAtLeastAsync(64, cancellationToken).ConfigureAwait(false);
            ReadHeaderResult(result);

            while (true)
            {
                result = await _pipeReader.ReadAsync(cancellationToken).ConfigureAwait(false);

                if ((_globalOffset + result.Buffer.Length) > _checkpointStartOffset)
                {
                    // We have read past the checkpoint registry, we can stop reading
                    // Advance to start of the registry
                    var positionInSequence = _checkpointStartOffset - _globalOffset;
                    _globalOffset += (int)positionInSequence;
                    var pos = result.Buffer.GetPosition(positionInSequence);
                    _pipeReader.AdvanceTo(pos);
                    break;
                }
                _globalOffset += (int)result.Buffer.Length;
                _pipeReader.AdvanceTo(result.Buffer.End);
            }

            while (true)
            {
                result = await _pipeReader.ReadAsync(cancellationToken).ConfigureAwait(false);

                if ((_globalOffset + result.Buffer.Length) > _checkpointRegistryStartOffset)
                {
                    var positionInSequence = _checkpointRegistryStartOffset - _globalOffset;
                    _globalOffset += (int)positionInSequence;
                    var pos = result.Buffer.GetPosition(positionInSequence);
                    return result.Buffer.Slice(result.Buffer.Start, pos);
                }

                //_globalOffset += (int)result.Buffer.Length;
                _pipeReader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
            }
        }

        public static ValueTask<CheckpointRegistryFile> ReadRegistryAsync(PipeReader pipeReader, IMemoryAllocator memoryAllocator, CancellationToken cancellationToken)
        {
            var reader = new BundleFileRegistryReader(pipeReader);
            return reader.ReadRegistryAsync(memoryAllocator, cancellationToken);
        }

        private async ValueTask<CheckpointRegistryFile> ReadRegistryAsync(IMemoryAllocator memoryAllocator, CancellationToken cancellationToken)
        {
            var result = await _pipeReader.ReadAtLeastAsync(64, cancellationToken).ConfigureAwait(false);
            ReadHeaderResult(result);

            while (true)
            {
                result = await _pipeReader.ReadAsync(cancellationToken).ConfigureAwait(false);

                if ((_globalOffset + result.Buffer.Length) > _checkpointRegistryStartOffset)
                {
                    // We have read past the checkpoint registry, we can stop reading
                    // Advance to start of the registry
                    var positionInSequence = _checkpointRegistryStartOffset - _globalOffset;
                    _globalOffset += (int)positionInSequence;
                    var pos = result.Buffer.GetPosition(positionInSequence);
                    _pipeReader.AdvanceTo(pos);
                    break;
                }
                else if (result.IsCompleted)
                {
                    throw new InvalidOperationException("Reached end of file before reaching checkpoint registry start offset.");
                }
                _globalOffset += (int)result.Buffer.Length;
                _pipeReader.AdvanceTo(result.Buffer.End);
            }

            // Read the entire file that is left into a buffer
            ReadResult readResult;
            do
            {
                readResult = await _pipeReader.ReadAsync(cancellationToken).ConfigureAwait(false);
                _pipeReader.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
            } while (!readResult.IsCompleted);

            var registry = CheckpointRegistryFile.Deserialize(readResult.Buffer, memoryAllocator);
            await _pipeReader.CompleteAsync().ConfigureAwait(false);
            return registry;
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

            if (!sequenceReader.TryReadLittleEndian(out short flags))
            {
                throw new InvalidOperationException("Failed to read flags from data file.");
            }

            if (!sequenceReader.TryReadLittleEndian(out int _))
            {
                throw new InvalidOperationException("Failed to read page count from data file.");
            }

            if (!sequenceReader.TryReadLittleEndian(out int _))
            {
                throw new InvalidOperationException("Failed to read page ids offset from data file.");
            }

            if (!sequenceReader.TryReadLittleEndian(out int _))
            {
                throw new InvalidOperationException("Failed to read page offsets offset from data file.");
            }

            if (!sequenceReader.TryReadLittleEndian(out int _))
            {
                throw new InvalidOperationException("Failed to read data start offset from data file.");
            }

            if ((flags & 1) == 1)
            {
                // This is a bundle file
                if (!sequenceReader.TryReadLittleEndian(out _checkpointStartOffset))
                {
                    throw new InvalidOperationException("Failed to read checkpoint start offset from data file.");
                }
                if (!sequenceReader.TryReadLittleEndian(out _checkpointRegistryStartOffset))
                {
                    throw new InvalidOperationException("Failed to read checkpoint registry start offset from data file.");
                }
                if (!sequenceReader.TryReadLittleEndian(out _registryFooterOffset))
                {
                    throw new InvalidOperationException("Failed to read registry footer offset from data file.");
                }
                // Skip reserved bytes
                sequenceReader.Advance(16);
            }
            else
            {
                throw new InvalidOperationException("Data file is not a bundle file, no registry information available.");
            }

            // Advance the pipe reader
            _pipeReader.AdvanceTo(sequenceReader.Position);
            _globalOffset = 64;
        }
    }
}
