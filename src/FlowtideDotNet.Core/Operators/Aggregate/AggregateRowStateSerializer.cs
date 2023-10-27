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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Aggregate
{
    internal class AggregateRowStateSerializer : IBplusTreeSerializer<AggregateRowState>
    {
        public AggregateRowState Deserialize(in BinaryReader reader)
        {
            var measureLength = reader.ReadInt32();
            var measureStates = new byte[measureLength][];
            for (int i = 0; i < measureLength; i++)
            {
                var length = reader.ReadInt32();
                measureStates[i] = reader.ReadBytes(length);
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

        public void Serialize(in BinaryWriter writer, in AggregateRowState value)
        {
            writer.Write(value.MeasureStates.Length);
            foreach (var measureState in value.MeasureStates)
            {
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
    }
}
