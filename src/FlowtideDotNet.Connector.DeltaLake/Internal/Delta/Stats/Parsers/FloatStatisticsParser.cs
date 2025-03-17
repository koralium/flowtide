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
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Parsers
{
    internal class FloatStatisticsParser : IStatisticsParser
    {
        private double? _minValue;
        private double? _maxValue;
        private int _nullCount;

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new FloatStatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public IDataValue GetValue(ref Utf8JsonReader reader)
        {
            var val = reader.GetDouble();
            return new DoubleValue(val);
        }

        public void ReadMaxValue(ref Utf8JsonReader reader)
        {
            _maxValue = reader.GetDouble();
        }

        public void ReadMinValue(ref Utf8JsonReader reader)
        {
            _minValue = reader.GetDouble();
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
                writer.WriteNumberValue(value.AsDouble);
            }
        }
    }
}
