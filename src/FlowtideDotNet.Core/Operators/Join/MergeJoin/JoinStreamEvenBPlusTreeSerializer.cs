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
using FlexBuffers;
using System.Buffers;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class JoinStreamEvenBPlusTreeSerializer : IBplusTreeSerializer<JoinStreamEvent>
    {
        public JoinStreamEvent Deserialize(in BinaryReader reader)
        {
            var targetId = reader.ReadByte();
            var iteration = reader.ReadUInt32();
            var hash = reader.ReadUInt64();
            var length = reader.ReadInt32();
            var bytes = reader.ReadBytes(length);
            var vector = FlxValue.FromMemory(bytes).AsVector;
            return new JoinStreamEvent(iteration, targetId, hash, new CompactRowData(bytes, vector));
        }

        public void Serialize(in BinaryWriter writer, in JoinStreamEvent value)
        {
            writer.Write(value.TargetId);
            writer.Write(value.Iteration);
            writer.Write(value.Hash);

            var rowEv = new RowEvent(0, 0, value.RowData);
            // TODO: This can be done much better
            var compactEvent = rowEv.Compact(new FlexBuffer(ArrayPool<byte>.Shared));
            var compactRowData = (CompactRowData)compactEvent.RowData;
            writer.Write(compactRowData.Span.Length);
            writer.Write(compactRowData.Span);
        }
    }
}
