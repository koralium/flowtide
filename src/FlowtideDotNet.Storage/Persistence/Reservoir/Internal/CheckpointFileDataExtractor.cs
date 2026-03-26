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
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal static class CheckpointFileDataExtractor
    {
        public static ReadOnlySequence<byte> GetCheckpointData(ReadOnlySequence<byte> bundleData)
        {
            var reader = new SequenceReader<byte>(bundleData);

            if (!reader.TryReadLittleEndian(out int magicNumber))
            {
                throw new InvalidOperationException("Could not read magic number on checkpoint file");
            }

            if (magicNumber == MagicNumbers.CompressedZstdCheckpointFileMagicNumber)
            {
                return bundleData;
            }
            else if (magicNumber == MagicNumbers.CheckpointFileMagicNumber)
            {
                return bundleData;
            }
            else if (magicNumber == MagicNumbers.BundleCheckpointFileMagicNumber)
            {
                reader.Advance(4);
                reader.TryReadLittleEndian(out long checkpointStart);
                reader.TryReadLittleEndian(out long registryStart);
                return bundleData.Slice(checkpointStart, registryStart - checkpointStart);
            }
            else
            {
                throw new InvalidOperationException($"Unexpected magic number {magicNumber} on checkpoint file");
            }
        }
    }
}
