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

using FlexBuffers;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Core.Operators.Write
{
    internal class GroupedStreamEventBPlusTreeSerializer : IBplusTreeSerializer<GroupedStreamEvent>
    {
        private static GroupedStreamEvent Deserialize(in BinaryReader reader)
        {
            var targetId = reader.ReadByte();
            var length = reader.ReadInt32();
            var bytes = reader.ReadBytes(length);
            var vector = FlxValue.FromMemory(bytes).AsVector;
            return new GroupedStreamEvent(targetId, new CompactRowData(bytes, vector));
        }

        public void Deserialize(in BinaryReader reader, in List<GroupedStreamEvent> values)
        {
            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                values.Add(Deserialize(reader));
            }
        }

        private static void Serialize(in BinaryWriter writer, in GroupedStreamEvent value)
        {
            writer.Write(value.TargetId);

            // TODO: Can be improved
            var compactData = (CompactRowData)new RowEvent(0, 0, value.RowData).Compact(new FlexBuffer(ArrayPool<byte>.Shared)).RowData;

            writer.Write(compactData.Span.Length);
            writer.Write(compactData.Span);
        }

        public void Serialize(in BinaryWriter writer, in List<GroupedStreamEvent> values)
        {
            writer.Write(values.Count);
            foreach (var value in values)
            {
                Serialize(writer, value);
            }
        }
    }
}
