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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore.Json
{
    internal static class DataValueJsonReader
    {
        public static IDataValue Read(ref Utf8JsonReader reader)
        {
            while (reader.TokenType == JsonTokenType.Comment || reader.TokenType == JsonTokenType.None)
            {
                if (!reader.Read())
                {
                    throw new JsonException("Unexpected json");
                }
                continue;
            }
            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                    var strVal = reader.GetString();
                    if (strVal == null)
                    {
                        return NullValue.Instance;
                    }
                    return new StringValue(strVal);
                case JsonTokenType.False:
                    return new BoolValue(false);
                case JsonTokenType.True:
                    return new BoolValue(true);
                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out var intVal))
                    {
                        return new Int64Value(intVal);
                    }
                    if (reader.TryGetDouble(out var doubleVal))
                    {
                        return new DoubleValue(doubleVal);
                    }
                    throw new JsonException();
                case JsonTokenType.Null:
                    return NullValue.Instance;
                case JsonTokenType.StartObject:
                    return ParseObject(ref reader);
                case JsonTokenType.StartArray:
                    return ParseArray(ref reader);
            }
            throw new NotImplementedException();
        }

        private static IDataValue ParseArray(ref Utf8JsonReader reader)
        {
            if (!reader.Read())
            {
                throw new JsonException("Unexpected json");
            }

            List<IDataValue> values = new List<IDataValue>();
            while (true)
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                {
                    break;
                }

                values.Add(Read(ref reader));

                if (!reader.Read())
                {
                    throw new JsonException("Unexpected json");
                }
            }

            return new ListValue(values);
        }

        private static IDataValue ParseObject(ref Utf8JsonReader reader)
        {
            if (!reader.Read())
            {
                throw new JsonException("Unexpected json");
            }

            List<KeyValuePair<IDataValue, IDataValue>> properties = new List<KeyValuePair<IDataValue, IDataValue>>();
            while (true)
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }
                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException();
                }

                var propertyName = reader.GetString();

                if (propertyName == null)
                {
                    throw new JsonException("Unexpected json");
                }

                if (!reader.Read())
                {
                    throw new JsonException("Unexpected json");
                }

                var value = Read(ref reader);

                if (!reader.Read())
                {
                    throw new JsonException("Unexpected json");
                }

                properties.Add(new KeyValuePair<IDataValue, IDataValue>(new StringValue(propertyName), value));
            }

            return new MapValue(properties);
        }
    }
}
