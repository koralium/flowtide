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

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters
{
    internal class UnionConverter : IObjectColumnConverter
    {
        private readonly ObjectConverterResolver resolver;
        private Dictionary<Type, IObjectColumnConverter> typeConverters;

        public UnionConverter(ObjectConverterResolver resolver)
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
            if (value.IsNull)
            {
                return null!;
            }

            switch (value.Type)
            {
                case ArrowTypeId.Int64:
                    return GetConverter(typeof(long)).Deserialize(value);
                case ArrowTypeId.String:
                    return GetConverter(typeof(string)).Deserialize(value);
                case ArrowTypeId.Boolean:
                    return GetConverter(typeof(bool)).Deserialize(value);
                case ArrowTypeId.Double:
                    return GetConverter(typeof(double)).Deserialize(value);
                case ArrowTypeId.Timestamp:
                    return GetConverter(typeof(DateTime)).Deserialize(value);
                case ArrowTypeId.Binary:
                    return GetConverter(typeof(byte[])).Deserialize(value);
                case ArrowTypeId.List:
                    return GetConverter(typeof(List<object>)).Deserialize(value);
                case ArrowTypeId.Map:
                    return GetConverter(typeof(Dictionary<object, object>)).Deserialize(value);
                case ArrowTypeId.Decimal128:
                    return GetConverter(typeof(decimal)).Deserialize(value);
                default:
                    throw new NotImplementedException();
            }
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            if (obj == null)
            {
                addFunc.AddValue(NullValue.Instance);
                return;
            }

            var type = obj.GetType();

            var converter = GetConverter(type);

            converter.Serialize(obj, ref addFunc);
        }
    }
}
