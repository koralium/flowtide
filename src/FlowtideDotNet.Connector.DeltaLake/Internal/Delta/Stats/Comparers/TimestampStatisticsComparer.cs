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

using FlowtideDotNet.Core.ColumnStore;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers
{
    internal class TimestampStatisticsComparer : IStatisticsComparer
    {
        private DateTimeOffset? _minValue;
        private DateTimeOffset? _maxValue;
        private readonly long? _minMilliseconds;
        private readonly long? _maxMilliseconds;
        private readonly int? _nullCount;

        public TimestampStatisticsComparer(DateTimeOffset? minValue, DateTimeOffset? maxValue, int? nullCount)
        {
            // Bounds and probes compare at the stored millisecond precision
            if (minValue.HasValue)
            {
                _minMilliseconds = StoredValue.TimestampMilliseconds(minValue.Value);
                _minValue = StoredValue.Timestamp(minValue.Value);
            }
            if (maxValue.HasValue)
            {
                _maxMilliseconds = StoredValue.TimestampMilliseconds(maxValue.Value);
                _maxValue = StoredValue.Timestamp(maxValue.Value);
            }
            _nullCount = nullCount;
        }

        public bool IsInBetween<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                if ((!_nullCount.HasValue) || _nullCount.Value > 0)
                {
                    return true;
                }
                return false;
            }
            var probe = StoredValue.TimestampMilliseconds(value.AsTimestamp);

            if (_minMilliseconds.HasValue && _minMilliseconds.Value > probe)
            {
                return false;
            }
            if (_maxMilliseconds.HasValue && _maxMilliseconds.Value < probe)
            {
                return false;
            }
            return true;
        }

        public void WriteMaxValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_maxValue != null)
            {
                writer.WriteString(propertyName, _maxValue.Value);
            }
        }

        public void WriteMinValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_minValue != null)
            {
                writer.WriteString(propertyName, _minValue.Value);
            }
        }

        public void WriteNullValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_nullCount.HasValue)
            {
                writer.WriteNumber(propertyName, _nullCount.Value);
            }
        }
    }
}
