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
using System.Buffers.Binary;
using System.IO.Hashing;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal static class DeletionVectorWriter
    {
        // An entry's size and checksum around its data
        public const int FrameOverhead = 8;

        private const int MagicNumber = 1681511377;

        public static (string path, string z85string) GenerateDestination()
        {
            var guid = Guid.NewGuid();
            var str = Z85.EncodeGuid(guid);

            var path = $"deletion_vector_{guid.ToString()}.bin";

            return (path, str);
        }

        // One entry: size, magic number and bitmap, checksum
        public static byte[] Frame(RoaringBitmapArray array)
        {
            using var memoryStream = new MemoryStream();
            using var writer = new BinaryWriter(memoryStream);

            writer.Write(0);
            writer.Write(MagicNumber);
            array.Serialize(writer);

            // dataSize is magic number (4 bytes) + roaring bitmap size
            var dataSize = (int)memoryStream.Length - 4;
            writer.Write(0);

            var frame = memoryStream.ToArray();
            BinaryPrimitives.WriteInt32BigEndian(frame, dataSize);
            BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(4 + dataSize), Crc32.HashToUInt32(frame.AsSpan(4, dataSize)));
            return frame;
        }
    }
}
