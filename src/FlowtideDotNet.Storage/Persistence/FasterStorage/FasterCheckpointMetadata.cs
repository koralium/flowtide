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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.FasterStorage
{
    internal struct CheckpointInfo
    {
        public Guid checkpointToken;
        public long checkpointVersion;
    }
    internal class FasterCheckpointMetadata
    {
        public long LastCheckpointVersion { get; }
        public byte[]? Metadata { get; }
        public long ChangesSinceLastCompact { get; set; } = 0;
        public List<CheckpointInfo> PreviousCheckpoints { get; }

        public FasterCheckpointMetadata(byte[]? metadata, long lastCheckpointVersion, long changesSinceLastCompact, List<CheckpointInfo> previousCheckpoints)
        {
            Metadata = metadata;
            PreviousCheckpoints = previousCheckpoints;
            LastCheckpointVersion = lastCheckpointVersion;
            ChangesSinceLastCompact = changesSinceLastCompact;
        }

        public FasterCheckpointMetadata Update(byte[] newMetadata, long lastCheckpointVersion)
        {
            return new FasterCheckpointMetadata(newMetadata, lastCheckpointVersion, ChangesSinceLastCompact, PreviousCheckpoints);
        }

        public void Serialize(IBufferWriter<byte> bufferWriter)
        {
            if (Metadata == null)
            {
                throw new InvalidOperationException("Metadata cannot be null when serializing FasterCheckpointMetadata.");
            }

            var writeLength = 8 + 8 + 4 + Metadata.Length + 4 + (24 * PreviousCheckpoints.Count);
            var span = bufferWriter.GetSpan(writeLength);

            BinaryPrimitives.WriteInt64LittleEndian(span, LastCheckpointVersion);
            span = span.Slice(8);

            // Write the changes since last compact
            BinaryPrimitives.WriteInt64LittleEndian(span, ChangesSinceLastCompact);
            span = span.Slice(8);

            // Write the length of the metadata
            BinaryPrimitives.WriteInt32LittleEndian(span, Metadata.Length);
            span = span.Slice(4);
            // Write the metadata
            Metadata.CopyTo(span);
            span = span.Slice(Metadata.Length);

            // Write the number of previous checkpoints
            BinaryPrimitives.WriteInt32LittleEndian(span, PreviousCheckpoints.Count);
            span = span.Slice(4);
            // Write each previous checkpoint
            foreach (var checkpoint in PreviousCheckpoints)
            {
                checkpoint.checkpointToken.TryWriteBytes(span);
                span = span.Slice(16);
                BinaryPrimitives.WriteInt64LittleEndian(span, checkpoint.checkpointVersion);
                span = span.Slice(8);
            }
            // Advance the buffer writer
            bufferWriter.Advance(writeLength);
        }

        public static FasterCheckpointMetadata Deserialize(ReadOnlySequence<byte> sequence)
        {
            var reader = new SequenceReader<byte>(sequence);

            if (!reader.TryReadLittleEndian(out long lastCheckpointVersion))
            {
                throw new InvalidOperationException("Invalid last checkpoint version");
            }

            // Read the changes since last compact
            if (!reader.TryReadLittleEndian(out long changesSinceLastCompact))
            {
                throw new InvalidOperationException("Invalid changes since last compact");
            }

            // Read the length of the metadata
            if (!reader.TryReadLittleEndian(out int metadataLength) || metadataLength < 0)
            {
                throw new InvalidOperationException("Invalid metadata length");
            }

            byte[] metadata = new byte[metadataLength];

            if (!reader.TryCopyTo(metadata))
            {
                throw new InvalidOperationException("Invalid metadata content");
            }
            reader.Advance(metadataLength);

            // Read the number of previous checkpoints
            if (!reader.TryReadLittleEndian(out int previousCheckpointsCount) || previousCheckpointsCount < 0)
            {
                throw new InvalidOperationException("Invalid previous checkpoints count");
            }

            byte[] tokenBytes = new byte[16];

            var previousCheckpoints = new List<CheckpointInfo>(previousCheckpointsCount);
            for (int i = 0; i < previousCheckpointsCount; i++)
            {
                
                if (!reader.TryCopyTo(tokenBytes))
                {
                    throw new InvalidOperationException("Invalid checkpoint token");
                }
                reader.Advance(16);

                var checkpointToken = new Guid(tokenBytes);

                if (!reader.TryReadLittleEndian(out long checkpointVersion))
                {
                    throw new InvalidOperationException("Invalid checkpoint version");
                }

                previousCheckpoints.Add(new CheckpointInfo
                {
                    checkpointToken = checkpointToken,
                    checkpointVersion = checkpointVersion
                });
            }

            return new FasterCheckpointMetadata(metadata, lastCheckpointVersion, changesSinceLastCompact, previousCheckpoints);
        }
    }
}
