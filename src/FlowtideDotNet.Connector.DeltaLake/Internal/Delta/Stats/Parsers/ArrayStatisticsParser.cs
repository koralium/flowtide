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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Parsers
{
    internal class ArrayStatisticsParser : IStatisticsParser
    {
        private IStatisticsParser _inner;

        public ArrayStatisticsParser(IStatisticsParser inner)
        {
            _inner = inner;
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            throw new NotImplementedException();
        }

        public IDataValue GetValue(ref Utf8JsonReader reader)
        {

            List<IDataValue> values = new List<IDataValue>();
            while (true)
            {
                reader.Read();

                if (reader.TokenType == JsonTokenType.EndArray)
                {
                    break;
                }

                var val = _inner.GetValue(ref reader);
                values.Add(val);
            }

            return new ListValue(values);
        }

        public void ReadMaxValue(ref Utf8JsonReader reader)
        {
            throw new NotImplementedException();
        }

        public void ReadMinValue(ref Utf8JsonReader reader)
        {
            throw new NotImplementedException();
        }

        public void ReadNullValue(ref Utf8JsonReader reader)
        {
            throw new NotImplementedException();
        }

        public void WriteValue<T>(Utf8JsonWriter writer, T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }
    }
}
