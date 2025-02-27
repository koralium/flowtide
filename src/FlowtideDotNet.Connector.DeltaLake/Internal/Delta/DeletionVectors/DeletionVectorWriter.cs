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

using FASTER.core;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        public static async Task<int> WriteDeletionVector(IFileStorage storage, IOPath table, string fileName, RoaringBitmapArray array)
        {
            using var stream = await storage.OpenWrite(table.Combine(fileName));

            if (stream == null)
            {
                throw new Exception("Failed to open stream");
            }

            using MemoryStream memoryStream = new MemoryStream();
            using var writer = new BinaryWriter(memoryStream);

            array.Serialize(writer);

            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes, (int)memoryStream.Position);

            stream.WriteByte(1);
            // Write data size
            stream.Write(bytes);

            BinaryPrimitives.WriteInt32LittleEndian(bytes, 1681511377);
            stream.Write(bytes);

            memoryStream.Position = 0;
            await memoryStream.CopyToAsync(stream);

            await stream.FlushAsync();

            int fileSize = (int)stream.Position;

            writer.Close();
            stream.Close();

            return fileSize;
        }
    }
}
