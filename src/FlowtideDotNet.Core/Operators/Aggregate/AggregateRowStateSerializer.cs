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

namespace FlowtideDotNet.Core.Operators.Aggregate
{
    internal class AggregateRowStateSerializer : IBplusTreeSerializer<AggregateRowState>
    {

        private static AggregateRowState Deserialize(in BinaryReader reader)
        {
            var measureLength = reader.ReadInt32();
            byte[]?[] measureStates = new byte[measureLength][];
            for (int i = 0; i < measureLength; i++)
            {
                var length = reader.ReadInt32();
                if (length == 0)
                {
                    measureStates[i] = null;
                }
                else
                {
                    measureStates[i] = reader.ReadBytes(length);
                }
                
            }
            var weight = reader.ReadInt64();
            var previousLength = reader.ReadInt32();
            byte[]? previousValue = null;
            if (previousLength > 0)
            {
                previousValue = reader.ReadBytes(previousLength);
            }
            return new AggregateRowState()
            {
                MeasureStates = measureStates,
                PreviousValue = previousValue,
                Weight = weight  
            };
        }

        private static void Serialize(in BinaryWriter writer, in AggregateRowState value)
        {
            writer.Write(value.MeasureStates.Length);
            foreach (var measureState in value.MeasureStates)
            {
                if (measureState == null)
                {
                    writer.Write(0);
                    continue;
                }
                writer.Write(measureState.Length);
                writer.Write(measureState);
            }
            writer.Write(value.Weight);
            if (value.PreviousValue == null)
            {
                writer.Write(0);
            }
            else
            {
                writer.Write(value.PreviousValue.Length);
                writer.Write(value.PreviousValue);
            }
        }

        public void Deserialize(in BinaryReader reader, in List<AggregateRowState> values)
        {
            var count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                values.Add(Deserialize(reader));
            }
        }

        public void Serialize(in BinaryWriter writer, in List<AggregateRowState> values)
        {
            writer.Write(values.Count);
            foreach (var value in values)
            {
                Serialize(writer, value);
            }
        }
    }
}
