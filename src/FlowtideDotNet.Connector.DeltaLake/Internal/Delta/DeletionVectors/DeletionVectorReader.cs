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
using Stowage;
using System.Buffers.Binary;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    // Fetches a packed file once per run
    internal sealed class DeletionVectorReader
    {
        // Larger foreign files are fetched once per vector
        private const long KeptBytes = 16L << 20;

        private readonly IFileStorage _storage;
        private readonly IOPath _table;
        private readonly Dictionary<string, byte[]> _files = new Dictionary<string, byte[]>();
        private long _bytes;

        public DeletionVectorReader(IFileStorage storage, IOPath table)
        {
            _storage = storage;
            _table = table;
        }

        public async Task<RoaringBitmapArray> Read(DeletionVector vector)
        {
            if (vector.StorageType == "u" || vector.StorageType == "p")
            {
                var path = vector.AbsolutePath!;
                if (_files.TryGetValue(path, out var kept))
                {
                    return ReadVector(new MemoryStream(kept, false), vector.Offset);
                }
                var deletionVectorStream = await _storage.OpenRead(_table.Combine(path));
                if (deletionVectorStream == null)
                {
                    throw new DeltaFileNotFoundException(path);
                }
                byte[] file;
                await using (deletionVectorStream)
                {
                    if (!deletionVectorStream.CanSeek || deletionVectorStream.Length > KeptBytes)
                    {
                        return ReadVector(deletionVectorStream, vector.Offset);
                    }
                    if (_bytes + deletionVectorStream.Length > KeptBytes)
                    {
                        _files.Clear();
                        _bytes = 0;
                    }
                    file = new byte[deletionVectorStream.Length];
                    await deletionVectorStream.ReadExactlyAsync(file);
                }
                var read = ReadVector(new MemoryStream(file, false), vector.Offset);
                // Kept only once a vector reads from it
                _files.Add(path, file);
                _bytes += file.Length;
                return read;
            }
            throw new NotImplementedException();
        }

        private static RoaringBitmapArray ReadVector(Stream stream, long? offset)
        {
            using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

            var version = reader.ReadByte();

            long start = 0;

            if (offset.HasValue)
            {
                start = offset.Value - 1;
            }

            if (stream.CanSeek && start > 0)
            {
                stream.Seek(start, SeekOrigin.Current);
            }
            else
            {
                for (long i = 0; i < start; i++)
                {
                    reader.ReadByte();
                }
            }

            _ = BinaryPrimitives.ReadInt32BigEndian(reader.ReadBytes(4));
            _ = reader.ReadBytes(4);

            return RoaringBitmapArray.Deserialize(reader);
        }
    }
}
