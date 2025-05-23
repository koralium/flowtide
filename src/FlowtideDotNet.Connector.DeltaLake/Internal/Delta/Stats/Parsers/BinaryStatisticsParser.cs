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
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Parsers
{
    internal class BinaryStatisticsParser : IStatisticsParser
    {
        private byte[]? _minValue;
        private byte[]? _maxValue;
        private int _nullCount;

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new BinaryStatisticsComparer(_minValue, _maxValue, _nullCount);
        }

        public IDataValue GetValue(ref Utf8JsonReader reader)
        {
            var str = reader.GetString();

            if (str == null)
            {
                return new BinaryValue(null);
            }

            byte[] byteArray = Encoding.Unicode.GetBytes(str);
            return new BinaryValue(byteArray);
        }

        public void ReadMaxValue(ref Utf8JsonReader reader)
        {
            var str = reader.GetString();

            if (str == null)
            {
                _maxValue = null;
            }
            else
            {
                _maxValue = Encoding.Unicode.GetBytes(str);
            }
        }

        public void ReadMinValue(ref Utf8JsonReader reader)
        {
            var str = reader.GetString();

            if (str == null)
            {
                _minValue = null;
            }
            else
            {
                _minValue = Encoding.Unicode.GetBytes(str);
            }
        }

        public void ReadNullValue(ref Utf8JsonReader reader)
        {
            _nullCount = reader.GetInt32();
        }

        public void WriteValue<T>(Utf8JsonWriter writer, T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }
    }
}
