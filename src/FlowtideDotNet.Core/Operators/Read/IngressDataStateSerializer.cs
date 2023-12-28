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

namespace FlowtideDotNet.Core.Operators.Read
{
    internal class IngressDataStateSerializer : IBplusTreeSerializer<IngressData>
    {
        private static IngressData Deserialize(in BinaryReader reader)
        {
            var isDeleted = reader.ReadBoolean();
            var length = reader.ReadInt32();
            var bytes = reader.ReadBytes(length);
            return new IngressData(bytes, isDeleted);
        }

        public void Deserialize(in BinaryReader reader, in List<IngressData> values)
        {
            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                values.Add(Deserialize(reader));
            }
        }

        private static void Serialize(in BinaryWriter writer, in IngressData value)
        {
            writer.Write(value.IsDeleted);
            writer.Write(value.Span.Length);
            writer.Write(value.Span);
        }

        public void Serialize(in BinaryWriter writer, in List<IngressData> values)
        {
            writer.Write(values.Count);
            foreach (var value in values)
            {
                Serialize(writer, value);
            }
        }
    }
}
