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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Parsers
{
    internal class TimestampStatisticsParser : IStatisticsParser
    {
        private DateTimeOffset? _minValue;
        private DateTimeOffset? _maxValue;
        private int? _nullCount;

        public void Reset()
        {
            _minValue = null;
            _maxValue = null;
            _nullCount = null;
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new TimestampStatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public IDataValue GetValue(ref Utf8JsonReader reader)
        {
            var val = ReadTimestamp(ref reader);
            return new TimestampTzValue(val);
        }

        public void ReadMaxValue(ref Utf8JsonReader reader)
        {
            _maxValue = ReadTimestamp(ref reader);
        }

        public void ReadMinValue(ref Utf8JsonReader reader)
        {
            _minValue = ReadTimestamp(ref reader);
        }

        // Strings without an offset are UTC
        private static DateTimeOffset ReadTimestamp(ref Utf8JsonReader reader)
        {
            if (!reader.TryGetDateTimeOffset(out var value))
            {
                // Near the date limits only the UTC reading fits
                var dateTime = reader.GetDateTime();
                if (dateTime.Kind == DateTimeKind.Unspecified)
                {
                    return new DateTimeOffset(dateTime, TimeSpan.Zero);
                }
                return reader.GetDateTimeOffset();
            }
            Span<byte> buffer = stackalloc byte[64];
            var text = reader.ValueIsEscaped || reader.HasValueSequence ? buffer.Slice(0, reader.CopyString(buffer)) : reader.ValueSpan;
            if (HasOffset(text))
            {
                return value;
            }
            return new DateTimeOffset(value.DateTime, TimeSpan.Zero);
        }

        private static bool HasOffset(ReadOnlySpan<byte> text)
        {
            // The date part has dashes, skip it
            for (int i = 10; i < text.Length; i++)
            {
                var c = text[i];
                if (c == 'Z' || c == 'z' || c == '+' || c == '-')
                {
                    return true;
                }
            }
            return false;
        }

        public void ReadNullValue(ref Utf8JsonReader reader)
        {
            _nullCount = reader.GetInt32();
        }

        public void WriteValue<T>(Utf8JsonWriter writer, T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                writer.WriteNullValue();
            }
            else
            {
                writer.WriteStringValue(value.AsTimestamp.ToDateTimeOffset());
            }
        }
    }
}
