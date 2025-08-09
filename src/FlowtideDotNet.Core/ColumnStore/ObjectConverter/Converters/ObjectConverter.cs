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
using System.Diagnostics;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters
{
    internal class ObjectConverter : IObjectColumnConverter
    {
        private class ColumnInfo
        {
            public IObjectColumnConverter Converter { get; }

            public ObjectConverterPropertyInfo PropertyInfo { get; }

            public ColumnInfo(IObjectColumnConverter converter, ObjectConverterPropertyInfo propertyInfo)
            {
                Converter = converter;
                PropertyInfo = propertyInfo;
            }
        }

        private readonly ObjectConverterTypeInfo typeInfo;
        private readonly IReadOnlyList<IObjectColumnConverter> converters;
        private Dictionary<string, ColumnInfo> columns;
        private StructHeader structHeader;

        public ObjectConverter(ObjectConverterTypeInfo typeInfo, IReadOnlyList<IObjectColumnConverter> converters)
        {
            this.typeInfo = typeInfo;
            this.converters = converters;

            string[] columnNames = new string[typeInfo.Properties.Count];
            columns = new Dictionary<string, ColumnInfo>();
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                var property = typeInfo.Properties[i];
                columnNames[i] = property.Name!;
                columns[property.Name!] = new ColumnInfo(converters[i], property);
            }
            structHeader = StructHeader.Create(columnNames);
        }

        public object Deserialize<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return null!;
            }
            if (value.Type == ArrowTypeId.Map)
            {
                var map = value.AsMap;
                var mapLength = map.GetLength();
                var obj = typeInfo.CreateObject!();
                for (int i = 0; i < mapLength; i++)
                {
                    var key = map.GetKeyAt(i);
                    var valueObj = map.GetValueAt(i);
                    var keyAsString = key.AsString.ToString();
                    if (columns.TryGetValue(keyAsString, out var converter))
                    {
                        var propertyValue = converter.Converter.Deserialize(valueObj);
                        converter.PropertyInfo.SetFunc!(obj, propertyValue);
                    }
                }
                return obj;
            }
            else if (value.Type == ArrowTypeId.Struct)
            {
                var structval = value.AsStruct;
                var structLength = structval.Header.Count;
                var obj = typeInfo.CreateObject!();
                for (int i = 0; i < structLength; i++)
                {
                    var columnName = structval.Header.GetColumnName(i);
                    if (columns.TryGetValue(columnName, out var converter))
                    {
                        var propertyValue = converter.Converter.Deserialize(structval.GetAt(i));
                        converter.PropertyInfo.SetFunc!(obj, propertyValue);
                    }
                }
                return obj;
            }
            else
            {
                throw new NotImplementedException($"Can not deserialize {value.Type} to generic object");
            }
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            IDataValue[] values = new IDataValue[structHeader.Count];
            for (int i = 0; i < typeInfo.Properties.Count; i++)
            {
                var property = typeInfo.Properties[i];

                if (property.GetFunc == null)
                {
                    throw new InvalidOperationException("Cannot serialize object without a get function");
                }

                var value = property.GetFunc(obj);

                Debug.Assert(property.Name != null);

                if (value == null)
                {
                    values[i] = NullValue.Instance;
                }
                else
                {
                    var func = new AddToColumnFunc();
                    converters[i].Serialize(value, ref func);

                    if (func.BoxedValue == null)
                    {
                        throw new InvalidOperationException("Boxed value is null");
                    }

                    values[i] = func.BoxedValue;
                }

            }
            addFunc.AddValue(new StructValue(structHeader, values));
        }

        public SubstraitBaseType GetSubstraitType()
        {
            return new MapType(new AnyType(), new AnyType());
        }
    }
}
