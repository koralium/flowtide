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

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base
{
    public class LongWatermarkValue : AbstractWatermarkValue<LongWatermarkValue>
    {
        public override int TypeId => 1;

        public long Value { get; }

        public LongWatermarkValue(long value)
        {
            Value = value;
        }

        public override int Compare(LongWatermarkValue? other)
        {
            if (other is LongWatermarkValue otherLong)
            {
                return Value.CompareTo(otherLong.Value);
            }
            else if (other is null)
            {
                return 1; // This instance is greater than null
            }
            else
            {
                throw new ArgumentException("Cannot compare LongWatermarkValue with " + other.GetType().Name, nameof(other));
            }
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Value, base.GetHashCode());
        }

        public static LongWatermarkValue Create(long value)
        {
            return new LongWatermarkValue(value);
        }
    }

    internal class LongWatermarkValueSerializer : IWatermarkSerializer
    {
        public AbstractWatermarkValue Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out long val))
            {
                throw new InvalidOperationException("Failed to read long value from the buffer.");
            }
            return new LongWatermarkValue(val);
        }

        public void Serialize(AbstractWatermarkValue value, IBufferWriter<byte> writer)
        {
            if (value is LongWatermarkValue longWatermarkValue)
            {
                var span = writer.GetSpan(8);
                BinaryPrimitives.WriteInt64LittleEndian(span, longWatermarkValue.Value);
                writer.Advance(8);
            }
            else
            {
                throw new ArgumentException("Value must be of type LongWatermarkValue", nameof(value));
            }
        }
    }
}
