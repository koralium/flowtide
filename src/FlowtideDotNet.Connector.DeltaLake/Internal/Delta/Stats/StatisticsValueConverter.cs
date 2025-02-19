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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Core.ColumnStore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats
{
    internal class StatisticsValueConverter : JsonConverter<DeltaValueStatistics>
    {
        private readonly StructType schema;
        private Dictionary<string, IStatisticsParser> parsers;

        public StatisticsValueConverter(StructType schema)
        {
            this.schema = schema;
            parsers = new Dictionary<string, IStatisticsParser>(StringComparer.OrdinalIgnoreCase);
            var visitor = new DeltaStatisticsVisitor();

            foreach(var field in schema.Fields)
            {
                var parser = visitor.Visit(field.Type);
                parsers.Add(field.Name, parser);
            }
        }

        public override DeltaValueStatistics? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            Dictionary<string, IDataValue> result = new Dictionary<string, IDataValue>();

            while (true)
            {
                reader.Read();

                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException("Expected property name when parsing statistics");
                }

                var propertyName = reader.GetString();

                if (propertyName == null)
                {
                    throw new JsonException("Expected property name when parsing statistics");
                }

                if (!parsers.TryGetValue(propertyName, out var parser))
                {
                    throw new JsonException($"Unknown statistics field {propertyName}");
                }

                result.Add(propertyName, parser.GetValue(ref reader));
            }
            return new DeltaValueStatistics(result);
        }

        public override void Write(Utf8JsonWriter writer, DeltaValueStatistics value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            foreach(var kv in value.Values)
            {
                writer.WritePropertyName(kv.Key);
                var parser = parsers[kv.Key];
                parser.WriteValue(writer, kv.Value);
            }
            writer.WriteEndObject();
        }
    }
}
