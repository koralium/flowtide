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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Metadata.SchemaTypes
{
    internal class IcebergSchemaWriter : JsonConverter<SchemaBaseType>
    {
        public override SchemaBaseType? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                // Primitive type
                var type = reader.GetString();
                return type switch
                {
                    "binary" => new BinaryType(),
                    "boolean" => new BooleanType(),
                    "byte" => new ByteType(),
                    "date" => new DateType(),
                    "double" => new DoubleType(),
                    "float" => new FloatType(),
                    "integer" => new IntegerType(),
                    "long" => new LongType(),
                    "short" => new ShortType(),
                    "string" => new StringType(),
                    "timestamp" => new TimestampType(),
                    _ => DefaultHandling(type)
                };
            }

            if (reader.TokenType == JsonTokenType.StartObject)
            {
                return ReadObject(ref reader, options);
            }

            throw new NotImplementedException();
        }

        private SchemaBaseType DefaultHandling(string? type)
        {
            if (type != null && type.StartsWith("decimal"))
            {
                var firstLeftSlash = type.IndexOf('(');
                var firstRightSlash = type.IndexOf(')');
                var comma = type.IndexOf(',');
                if (firstLeftSlash == -1 || firstRightSlash == -1 || comma == -1)
                {
                    throw new JsonException("Invalid decimal type");
                }

                var precision = int.Parse(type.Substring(firstLeftSlash + 1, comma - firstLeftSlash - 1));
                var scale = int.Parse(type.Substring(comma + 1, firstRightSlash - comma - 1));

                return new DecimalType(precision, scale);
            }
            throw new JsonException($"Unknown type: {type}");
        }

        private SchemaBaseType ReadObject(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            reader.Read();

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException();
            }

            var propertyName = reader.GetString();
            if (propertyName != "type")
            {
                throw new JsonException();
            }

            reader.Read();

            if (reader.TokenType != JsonTokenType.String)
            {
                throw new JsonException();
            }

            var type = reader.GetString();
            return type switch
            {
                "struct" => ReadStruct(ref reader, options),
                "array" => ReadArray(ref reader, options),
                "map" => ReadMap(ref reader, options),
                _ => throw new JsonException($"Unknown type: {type}")
            };
        }

        private SchemaBaseType ReadStruct(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            reader.Read();

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException();
            }

            var propertyName = reader.GetString();
            if (propertyName != "fields")
            {
                throw new JsonException();
            }

            reader.Read();

            if (reader.TokenType != JsonTokenType.StartArray)
            {
                throw new JsonException();
            }

            var fields = new List<StructField>();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                {
                    break;
                }

                var field = JsonSerializer.Deserialize<StructField>(ref reader, options);
                fields.Add(field!);
            }

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }
            }

            return new StructType(fields);
        }

        private SchemaBaseType ReadArray(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            reader.Read();

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException();
            }

            var propertyName = reader.GetString();
            if (propertyName != "elementType")
            {
                throw new JsonException();
            }

            reader.Read();

            if (reader.TokenType != JsonTokenType.String)
            {
                throw new JsonException();
            }

            var elementType = JsonSerializer.Deserialize<SchemaBaseType>(ref reader, options);

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }
            }

            return new ArrayType()
            {
                ElementType = elementType
            };
        }

        private SchemaBaseType ReadMap(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            SchemaBaseType? keyType = default;
            SchemaBaseType? valueType = default;
            bool valueContainsNull = false;
            while (true)
            {
                reader.Read();

                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    break;
                }

                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException();
                }

                var propertyNameString = reader.GetString();

                if (propertyNameString == "keyType")
                {
                    reader.Read();
                    keyType = JsonSerializer.Deserialize<SchemaBaseType>(ref reader, options);
                }
                else if (propertyNameString == "valueType")
                {
                    reader.Read();
                    valueType = JsonSerializer.Deserialize<SchemaBaseType>(ref reader, options);
                }
                else if (propertyNameString == "valueContainsNull")
                {
                    reader.Read();
                    valueContainsNull = reader.GetBoolean();
                    // Do nothing right now
                }
                else
                {
                    throw new JsonException();
                }
            }

            if (keyType == null || valueType == null)
            {
                throw new JsonException("Map is missing key type or value type");
            }

            return new MapType(keyType, valueType, valueContainsNull);
        }

        public override void Write(Utf8JsonWriter writer, SchemaBaseType value, JsonSerializerOptions options)
        {
            if (value is BinaryType)
            {
                writer.WriteStringValue("binary");
            }
            else if (value is BooleanType)
            {
                writer.WriteStringValue("boolean");
            }
            else if (value is ByteType)
            {
                writer.WriteStringValue("byte");
            }

            else if (value is StringType)
            {
                writer.WriteStringValue("string");
            }
            else if (value is IntegerType)
            {
                writer.WriteStringValue("integer");
            }
            else if (value is LongType)
            {
                writer.WriteStringValue("long");
            }
            else if (value is ShortType)
            {
                writer.WriteStringValue("short");
            }
            else if (value is FloatType)
            {
                writer.WriteStringValue("float");
            }
            else if (value is DoubleType)
            {
                writer.WriteStringValue("double");
            }
            else if (value is DateType)
            {
                writer.WriteStringValue("date");
            }
            else if (value is DecimalType decimalType)
            {
                writer.WriteStringValue($"decimal({decimalType.Precision},{decimalType.Scale})");
            }
            else if (value is TimestampType)
            {
                writer.WriteStringValue("timestamp");
            }
            else if (value is StructType structType)
            {
                WriteStruct(writer, structType, options);
            }
            else if (value is ArrayType arrayType)
            {
                WriteList(writer, arrayType, options);
            }
            else if (value is MapType mapType)
            {
                WriteMap(writer, mapType, options);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private static void WriteList(Utf8JsonWriter writer, ArrayType value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("type", "list");

            if (!value.ElementId.HasValue)
            {
                throw new NotSupportedException("ElementId is required for Iceberg array type");
            }

            writer.WriteNumber("element-id", value.ElementId.Value);

            writer.WriteBoolean("element-required", !value.ContainsNull);

            writer.WritePropertyName("element");
            JsonSerializer.Serialize(writer, value.ElementType, options);

            writer.WriteEndObject();
        }

        private static void WriteStruct(Utf8JsonWriter writer, StructType value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            writer.WriteString("type", "struct");

            writer.WritePropertyName("fields");
            writer.WriteStartArray();
            foreach (var field in value.Fields)
            {
                writer.WriteStartObject();

                writer.WritePropertyName("id");
                if (!field.FieldId.HasValue)
                {
                    throw new NotSupportedException("Field ID is required for serialization.");
                }
                writer.WriteNumberValue(field.FieldId.Value);

                writer.WriteString("name", field.Name);

                writer.WriteBoolean("required", !field.Nullable);

                writer.WritePropertyName("type");

                JsonSerializer.Serialize(writer, field.Type, options);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        private static void WriteMap(Utf8JsonWriter writer, MapType value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            writer.WriteString("type", "map");

            if (!value.KeyId.HasValue)
            {
                throw new NotSupportedException("KeyId is required for Iceberg map type");
            }
            writer.WriteNumber("key-id", value.KeyId.Value);

            writer.WritePropertyName("key");
            JsonSerializer.Serialize(writer, value.KeyType, options);

            if (!value.ValueId.HasValue)
            {
                throw new NotSupportedException("ValueId is required for Iceberg map type");
            }

            writer.WriteNumber("value-id", value.ValueId.Value);

            writer.WriteBoolean("value-required", !value.ValueContainsNull);

            writer.WritePropertyName("value");
            JsonSerializer.Serialize(writer, value.ValueType, options);

            writer.WriteEndObject();
        }
    }
}
