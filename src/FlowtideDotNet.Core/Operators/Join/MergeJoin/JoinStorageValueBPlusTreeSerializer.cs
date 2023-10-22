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
        public JoinStorageValue Deserialize(in BinaryReader reader)
        {
            var bytes = reader.ReadBytes(8);
            var weight = BinaryPrimitives.ReadInt32LittleEndian(bytes);
            var joinWeight = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan().Slice(4));
            return new JoinStorageValue()
            {
                Weight = weight,
                JoinWeight = joinWeight
            };
        }

        public void Serialize(in BinaryWriter writer, in JoinStorageValue value)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, value.Weight);
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan().Slice(4), value.JoinWeight);
            writer.Write(bytes);
        }
    }
}
