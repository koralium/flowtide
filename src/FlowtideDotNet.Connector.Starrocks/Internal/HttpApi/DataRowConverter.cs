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

using System.Text.Json;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    internal class DataRowConverter : JsonConverter<List<object?>>
    {
        public override List<object?>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartArray)
            {
                throw new JsonException();
            }

            List<object?> data = new List<object?>();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                {
                    break;
                }
                data.Add(ReadDataValue(ref reader));
            }
            return data;
        }

        private static object? ReadDataValue(ref Utf8JsonReader reader)
        {
            if (reader.TokenType == JsonTokenType.Null)
            {
                // Handle null value
                return null;

            }
            else if (reader.TokenType == JsonTokenType.String)
            {
                var value = reader.GetString();
                return value;
            }
            else if (reader.TokenType == JsonTokenType.Number)
            {
                if (reader.TryGetInt64(out long longValue))
                {
                    // Handle long value
                    return longValue;
                }
                else if (reader.TryGetDouble(out double doubleValue))
                {
                    // Handle double value
                    return doubleValue;
                }
                else
                {
                    throw new InvalidOperationException("Unknown number format");
                }
            }
            else if (reader.TokenType == JsonTokenType.True || reader.TokenType == JsonTokenType.False)
            {
                var boolValue = reader.GetBoolean();
                return boolValue;
            }
            else
            {
                throw new InvalidOperationException("Unknown token type");
            }
        }

        public override void Write(Utf8JsonWriter writer, List<object?> value, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }
    }
}
