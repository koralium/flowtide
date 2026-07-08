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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System.Buffers.Binary;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal static class DeletionVectorWriter
    {
        public static (string path, string z85string) GenerateDestination()
        {
            var guid = Guid.NewGuid();
            var str = Z85.EncodeGuid(guid);

            var path = $"deletion_vector_{guid.ToString()}.bin";

            return (path, str);
        }

        public static async Task<(int fileSize, int dataSize)> WriteDeletionVector(IFileStorage storage, IOPath table, string fileName, RoaringBitmapArray array)
        {
            using var stream = await storage.OpenWrite(table.Combine(fileName));

            if (stream == null)
            {
                throw new Exception("Failed to open stream");
            }

            using var writeStream = new DeltaWriteStream(stream);

            using MemoryStream memoryStream = new MemoryStream();
            using var writer = new BinaryWriter(memoryStream);

            array.Serialize(writer);

            // dataSize is magic number (4 bytes) + roaring bitmap size
            var dataSize = 4 + (int)memoryStream.Position;
            var sizeBytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(sizeBytes, dataSize);

            writeStream.WriteByte(1);
            // Write data size
            writeStream.Write(sizeBytes);

            var magicBytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(magicBytes, 1681511377);
            writeStream.Write(magicBytes);

            memoryStream.Position = 0;
            await memoryStream.CopyToAsync(writeStream);

            // Compute CRC-32 of bitmapData (magic number + serialized bitmap)
            var crc = new System.IO.Hashing.Crc32();
            crc.Append(magicBytes);
            crc.Append(memoryStream.ToArray());

            var checksumValue = BinaryPrimitives.ReadUInt32LittleEndian(crc.GetCurrentHash());
            var checksumBytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(checksumBytes, checksumValue);
            writeStream.Write(checksumBytes);

            await writeStream.FlushAsync();

            int fileSize = (int)writeStream.Position;

            return (fileSize, dataSize);
        }
    }
}
