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
    internal class StructStatisticsParser : IStatisticsParser
    {
        private readonly Dictionary<string, IStatisticsParser> _parsers;

        public StructStatisticsParser(Dictionary<string, IStatisticsParser> parsers)
        {
            _parsers = parsers;
        }

        public IStatisticsComparer GetStatisticsComparer()
        {
            return new StructStatisticsComparer(_parsers.Select(x => new KeyValuePair<string, IStatisticsComparer>(x.Key, x.Value.GetStatisticsComparer())), default);
        }

        public IDataValue GetValue(ref Utf8JsonReader reader)
        {
            List<KeyValuePair<IDataValue, IDataValue>> values = new List<KeyValuePair<IDataValue, IDataValue>>();


            while (true)
            {
                reader.Read();

                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new Exception("Expected property name");
                }

                var propertyName = reader.GetString();

                if (!_parsers.TryGetValue(propertyName!, out var parser))
                {
                    throw new Exception($"No parser found for property {propertyName}");
                }
            }

            throw new NotImplementedException();
        }

        public void ReadMaxValue(ref Utf8JsonReader reader)
        {   
            while (true)
            {
                reader.Read();

                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException("Expected property name");
                }

                var propertyName = reader.GetString();

                if (propertyName == null)
                {
                    throw new JsonException("Expected property name");
                }

                reader.Read();

                if (!_parsers.TryGetValue(propertyName, out var parser))
                {
                    reader.Skip();
                    continue;
                }

                parser.ReadMaxValue(ref reader);
            }
        }

        public void ReadMinValue(ref Utf8JsonReader reader)
        {
            while (true)
            {
                reader.Read();

                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException("Expected property name");
                }

                var propertyName = reader.GetString();

                if (propertyName == null)
                {
                    throw new JsonException("Expected property name");
                }

                reader.Read();

                if (!_parsers.TryGetValue(propertyName, out var parser))
                {
                    reader.Skip();
                    continue;
                }

                parser.ReadMinValue(ref reader);
            }
        }

        public void ReadNullValue(ref Utf8JsonReader reader)
        {
            while (true)
            {
                reader.Read();

                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException("Expected property name");
                }

                var propertyName = reader.GetString();

                if (propertyName == null)
                {
                    throw new JsonException("Expected property name");
                }

                reader.Read();

                if (!_parsers.TryGetValue(propertyName, out var parser))
                {
                    reader.Skip();
                    continue;
                }

                parser.ReadNullValue(ref reader);
            }
        }
    }
}
