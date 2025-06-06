﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Parsers
{
    internal class StringStatisticsParser : IStatisticsParser
    {
        private string? _minValue;
        private string? _maxValue;
        private int _nullCount;

        public IStatisticsComparer GetStatisticsComparer()
        {
            byte[]? minValue = default;
            byte[]? maxValue = default;

            if (_minValue != null)
            {
                minValue = Encoding.UTF8.GetBytes(_minValue);
            }
            if (_maxValue != null)
            {
                maxValue = Encoding.UTF8.GetBytes(_maxValue);
            }

            return new StringStatisticsComparer(minValue, maxValue, _nullCount);
        }

        public IDataValue GetValue(ref Utf8JsonReader reader)
        {
            var str = reader.GetString();
            if (str == null)
            {
                return NullValue.Instance;
            }
            return new StringValue(str);
        }

        public void ReadMaxValue(ref Utf8JsonReader reader)
        {
            _maxValue = reader.GetString();
        }

        public void ReadMinValue(ref Utf8JsonReader reader)
        {
            _minValue = reader.GetString();
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
                writer.WriteStringValue(value.AsString.Span);
            }
        }
    }
}
