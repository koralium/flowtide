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

namespace FlowtideDotNet.Core.Storage
{
    public class StreamEventBPlusTreeSerializer : IBplusTreeSerializer<RowEvent>
    {
        public RowEvent Deserialize(in BinaryReader reader)
        {
            var weight = reader.ReadInt32();
            var iteration = reader.ReadUInt32();
            var length = reader.ReadInt32();
            var bytes = reader.ReadBytes(length);
            return new RowEvent(weight, iteration, new CompactRowData(bytes));
        }

        public void Deserialize(in BinaryReader reader, in List<RowEvent> values)
        {
            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                values.Add(Deserialize(reader));
            }
        }

        public void Serialize(in BinaryWriter writer, in RowEvent value)
        {
            writer.Write(value.Weight);
            writer.Write(value.Iteration);

            // TODO: Check if it already is a compact event with no emit, in that case this can be skipped
            var flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
            var compactRow = value.Compact(flexBuffer);
            var compact = (CompactRowData)compactRow.RowData;

            writer.Write(compact.Span.Length);
            writer.Write(compact.Span);
        }

        public void Serialize(in BinaryWriter writer, in List<RowEvent> values)
        {
            writer.Write(values.Count);
            foreach (var value in values)
            {
                Serialize(writer, value);
            }
        }
    }
}
