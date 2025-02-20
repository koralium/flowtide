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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats
{
    internal class DeltaStatisticsConverter : JsonConverter<DeltaStatistics>
    {
        private readonly StructType schema;
        private Dictionary<string, IStatisticsParser> parsers;

        public DeltaStatisticsConverter(StructType schema)
        {
            this.schema = schema;
            parsers = new Dictionary<string, IStatisticsParser>(StringComparer.OrdinalIgnoreCase);
            var visitor = new DeltaStatisticsVisitor();

            foreach (var field in schema.Fields)
            {
                var parser = visitor.Visit(field.Type);
                if (parser == null)
                {
                    continue;
                }
                parsers.Add(field.Name, parser);
            }
        }

        public override DeltaStatistics? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            int numRecords = 0;
            bool tightBounds = false;

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

                reader.Read();

                switch (propertyName)
                {
                    case "numRecords":
                        numRecords = reader.GetInt32();
                        break;
                    case "tightBounds":
                        tightBounds = reader.GetBoolean();
                        break;
                    case "minValues":
                        ParseMinValues(ref reader);
                        break;
                    case "maxValues":
                        ParseMaxValues(ref reader);
                        break;
                    case "nullCount":
                        ParseNullCounts(ref reader);
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }

            Dictionary<string, IStatisticsComparer> comparers = new Dictionary<string, IStatisticsComparer>();
            foreach(var kv in parsers)
            {
                comparers.Add(kv.Key, kv.Value.GetStatisticsComparer());
            }

            return new DeltaStatistics()
            {
                NumRecords = numRecords,
                TightBounds = tightBounds,
                ValueComparers = comparers
            };
        }

        private void ParseMinValues(ref Utf8JsonReader reader)
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

                if (!parsers.TryGetValue(propertyName, out var parser))
                {
                    reader.Skip();
                    continue;
                }

                parser.ReadMinValue(ref reader);
            }
        }

        private void ParseMaxValues(ref Utf8JsonReader reader)
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

                if (!parsers.TryGetValue(propertyName, out var parser))
                {
                    reader.Skip();
                    continue;
                }

                parser.ReadMaxValue(ref reader);
            }
        }

        private void ParseNullCounts(ref Utf8JsonReader reader)
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

                if (!parsers.TryGetValue(propertyName, out var parser))
                {
                    reader.Skip();
                    continue;
                }

                parser.ReadNullValue(ref reader);
            }
        }

        public override void Write(Utf8JsonWriter writer, DeltaStatistics value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            writer.WriteNumber("numRecords", value.NumRecords);
            writer.WriteBoolean("tightBounds", value.TightBounds);

            if (value.ValueComparers != null)
            {
                writer.WriteStartObject("minValues");

                foreach (var kv in value.ValueComparers)
                {
                    kv.Value.WriteMinValue(writer, kv.Key);
                }

                writer.WriteEndObject();

                writer.WriteStartObject("maxValues");

                foreach (var kv in value.ValueComparers)
                {
                    kv.Value.WriteMaxValue(writer, kv.Key);
                }

                writer.WriteEndObject();

                writer.WriteStartObject("nullCount");

                foreach (var kv in value.ValueComparers)
                {
                    kv.Value.WriteNullValue(writer, kv.Key);
                }

                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }
    }
}
