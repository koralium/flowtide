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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters
{
    internal class JsonElementConverter : IObjectColumnConverter
    {
        private readonly ObjectConverterResolver resolver;
        private Dictionary<Type, IObjectColumnConverter> typeConverters;

        public JsonElementConverter(ObjectConverterResolver resolver)
        {
            this.resolver = resolver;
            typeConverters = new Dictionary<Type, IObjectColumnConverter>();
        }

        private IObjectColumnConverter GetConverter(Type type)
        {
            if (!typeConverters.TryGetValue(type, out var converter))
            {
                converter = resolver.GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(type));
                typeConverters[type] = converter;
            }

            return converter;
        }

        public object Deserialize<T>(T value) where T : IDataValue
        {
            switch (value.Type)
            {
                case ArrowTypeId.Int64:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(long)).Deserialize(value));
                case ArrowTypeId.String:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(string)).Deserialize(value));
                case ArrowTypeId.Boolean:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(bool)).Deserialize(value));
                case ArrowTypeId.Double:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(double)).Deserialize(value));
                case ArrowTypeId.Timestamp:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(DateTime)).Deserialize(value));
                case ArrowTypeId.Binary:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(byte[])).Deserialize(value));
                case ArrowTypeId.List:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(List<object>)).Deserialize(value));
                case ArrowTypeId.Map:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(Dictionary<object, object>)).Deserialize(value));
                case ArrowTypeId.Decimal128:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(decimal)).Deserialize(value));
                case ArrowTypeId.Struct:
                    return JsonSerializer.SerializeToElement(GetConverter(typeof(Dictionary<string, object>)).Deserialize(value));
                default:
                    throw new NotImplementedException($"Can not deserialize {value.Type} to object");
            }
        }

        public SubstraitBaseType GetSubstraitType()
        {
            return new AnyType();
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            if (obj is JsonElement jsonElement)
            {
                switch (jsonElement.ValueKind)
                {
                    case JsonValueKind.String:
                        var str = jsonElement.GetString();

                        if (str != null)
                        {
                            addFunc.AddValue(new StringValue(str));
                        }
                        else
                        {
                            addFunc.AddValue(NullValue.Instance);
                        }
                        return;
                    case JsonValueKind.True:
                        addFunc.AddValue(new BoolValue(true));
                        return;
                    case JsonValueKind.False:
                        addFunc.AddValue(new BoolValue(false));
                        return;
                    case JsonValueKind.Number:
                        if (jsonElement.TryGetInt64(out var longValue))
                        {
                            addFunc.AddValue(new Int64Value(longValue));
                            return;
                        }
                        else if (jsonElement.TryGetDouble(out var doubleValue))
                        {
                            addFunc.AddValue(new DoubleValue(doubleValue));
                            return;
                        }
                        else
                        {
                            throw new NotImplementedException("Unsupported number type in JsonElement");
                        }
                    case JsonValueKind.Object:
                        var objConverter = GetConverter(typeof(Dictionary<string, object>));
                        objConverter.Serialize(JsonSerializer.Deserialize<Dictionary<string, object>>(jsonElement.GetRawText())!, ref addFunc);
                        return;
                    case JsonValueKind.Array:
                        var listConverter = GetConverter(typeof(List<object>));
                        listConverter.Serialize(JsonSerializer.Deserialize<List<object>>(jsonElement.GetRawText())!, ref addFunc);
                        return;
                    case JsonValueKind.Null:
                        addFunc.AddValue(NullValue.Instance);
                        return;
                    default:
                        throw new NotImplementedException($"Unsupported JsonElement kind: {jsonElement.ValueKind}");
                }
            }
            else
            {
                throw new InvalidOperationException("Object is not a JsonElement");
            }
        }
    }
}
