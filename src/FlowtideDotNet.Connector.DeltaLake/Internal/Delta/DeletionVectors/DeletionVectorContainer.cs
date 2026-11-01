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

using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    // A new vector file, buffered until written once
    internal sealed class DeletionVectorContainer
    {
        private readonly MemoryStream _file = new MemoryStream();
        private readonly string _id;

        public DeletionVectorContainer()
        {
            (Path, _id) = DeletionVectorWriter.GenerateDestination();
            _file.WriteByte(1);
        }

        public string Path { get; }

        public long Length => _file.Length;

        // The offset is the position of the entry's size
        public DeletionVector Add(byte[] frame, long cardinality)
        {
            var offset = _file.Length;
            _file.Write(frame);
            return new DeletionVector()
            {
                Cardinality = cardinality,
                Offset = offset,
                StorageType = "u",
                PathOrInlineDv = _id,
                SizeInBytes = frame.Length - DeletionVectorWriter.FrameOverhead
            };
        }

        public async Task WriteAsync(IFileStorage storage, IOPath table)
        {
            await using var stream = await storage.OpenWrite(table.Combine(Path));
            _file.Position = 0;
            await _file.CopyToAsync(stream);
            await stream.FlushAsync();
        }
    }
}
