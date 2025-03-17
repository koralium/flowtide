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

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class JoinStreamEvenBPlusTreeSerializer : IBplusTreeSerializer<JoinStreamEvent>
    {
        public void Deserialize(in BinaryReader reader, in List<JoinStreamEvent> values)
        {
            var byteCount = reader.ReadInt32();
            var bytes = reader.ReadBytes(byteCount);
            var vector = FlxValue.FromMemory(bytes).AsVector;

            for (var i = 0; i < vector.Length; i++)
            {
                values.Add(new JoinStreamEvent(0, 0, new VectorRowData(vector.Get(i).AsVector)));
            }
        }

        public void Serialize(in BinaryWriter writer, in List<JoinStreamEvent> values)
        {
            var builder = new FlexBuffer(ArrayPool<byte>.Shared, options: FlexBuffer.Options.ShareKeys | FlexBuffer.Options.ShareStrings | FlexBuffer.Options.ShareKeyVectors);
            builder.NewObject();
            var startRoot = builder.StartVector();
            foreach (var value in values)
            {
                var elementStart = builder.StartVector();
                for (int i = 0; i < value.Length; i++)
                {
                    builder.Add(value.GetColumn(i));
                }
                builder.EndVector(elementStart, false, false);
            }
            builder.EndVector(startRoot, false, false);
            var bytes = builder.Finish();
            writer.Write(bytes.Length);
            writer.Write(bytes);
        }
    }
}
