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
    internal class DecimalStatisticsComparer : IStatisticsComparer
    {
        private decimal? _minValue;
        private decimal? _maxValue;
        private readonly int? _nullCount;
        private const decimal Epsilon = 0.000000001m;

        public DecimalStatisticsComparer(decimal? minValue, decimal? maxValue, int? nullCount)
        {
            _minValue = minValue;
            _maxValue = maxValue;
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
            var decimalValue = value.AsDecimal;

            if (_minValue != null && (_minValue - Epsilon) > decimalValue)
            {
                return false;
            }

            if (_maxValue != null && (_maxValue + Epsilon) < decimalValue)
            {
                return false;
            }
            return true;
        }

        public void WriteMinValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_minValue != null)
            {
                writer.WriteNumber(propertyName, _minValue.Value);
            }
        }

        public void WriteMaxValue(Utf8JsonWriter writer, string propertyName)
        {
            if (_maxValue != null)
            {
                writer.WriteNumber(propertyName, _maxValue.Value);
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
