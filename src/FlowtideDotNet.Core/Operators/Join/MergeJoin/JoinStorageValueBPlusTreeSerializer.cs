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

using FlowtideDotNet.Core.Operators.Join.NestedLoopJoin;
using FlowtideDotNet.Storage.Tree;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class JoinStorageValueBPlusTreeSerializer : IBplusTreeSerializer<JoinStorageValue>
    {
        public void Deserialize(in BinaryReader reader, in List<JoinStorageValue> values)
        {
            var length = reader.ReadInt32();

            var bytes = reader.ReadBytes(8 * length);
            var span = bytes.AsSpan();
            for (int i = 0; i < length; i++)
            {
                var weight = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(i * 8));
                var joinWeight = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(i * 8 + 4));
                values.Add(new JoinStorageValue()
                {
                    Weight = weight,
                    JoinWeight = joinWeight
                });
            }
        }

        public void Serialize(in BinaryWriter writer, in List<JoinStorageValue> values)
        {
            writer.Write(values.Count);
            var bytes = new byte[8 * values.Count];
            var span = bytes.AsSpan();
            for (int i = 0; i < values.Count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(span.Slice(i * 8), values[i].Weight);
                BinaryPrimitives.WriteInt32LittleEndian(span.Slice(i * 8 + 4), values[i].JoinWeight);
            }
            writer.Write(bytes);
        }
    }
}
