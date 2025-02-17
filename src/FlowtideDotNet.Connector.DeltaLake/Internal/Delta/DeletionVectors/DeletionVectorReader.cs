using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using Stowage;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal static class DeletionVectorReader
    {
        public static async Task<RoaringBitmapArray> ReadDeletionVector(IFileStorage storage, IOPath table, DeletionVector vector)
        {
            if (vector.StorageType == "u" || vector.StorageType == "p")
            {
                var deletionVectorStream = await storage.OpenRead(table.Combine(vector.AbsolutePath));
                return ReadVector(deletionVectorStream!, vector.Offset);
            }
            throw new NotImplementedException();
        }

        private static RoaringBitmapArray ReadVector(Stream stream, long? offset)
        {
            using var reader = new BinaryReader(stream);

            var version = reader.ReadByte();

            long start = 0;

            if (offset.HasValue)
            {
                start = offset.Value - 1;
            }

            for (int i = 0; i < start; i++)
            {
                reader.ReadByte();
            }

            var dataSize = BinaryPrimitives.ReadInt32BigEndian(reader.ReadBytes(4));

            var magicNumberBytes = reader.ReadBytes(4);
            var magicNumber = BinaryPrimitives.ReadInt32LittleEndian(magicNumberBytes);

            return RoaringBitmapArray.Deserialize(reader);
        }
    }
}
