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

using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// Pruning rows of the spilled projection, stateless like the record serializer.
    /// </summary>
    internal sealed class BoundsRowSerializer : IBplusTreeValueSerializer<byte[], ManagedValueContainer<byte[]>>
    {
        private static readonly Func<byte[], int> s_sizeOf = x => x.Length + 16;

        public ManagedValueContainer<byte[]> CreateEmpty()
        {
            return new ManagedValueContainer<byte[]>(s_sizeOf);
        }

        public void Serialize(in IBufferWriter<byte> writer, in ManagedValueContainer<byte[]> values)
        {
            BinaryWriting.WriteInt32(writer, values.Count);
            foreach (var row in values.Values)
            {
                BinaryWriting.WriteBytes(writer, row);
            }
        }

        public ManagedValueContainer<byte[]> Deserialize(ref SequenceReader<byte> reader)
        {
            var count = BinaryReading.ReadInt32(ref reader);
            var container = CreateEmpty();
            for (int i = 0; i < count; i++)
            {
                container.Add(BinaryReading.ReadBytes(ref reader));
            }
            return container;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }
    }
}
