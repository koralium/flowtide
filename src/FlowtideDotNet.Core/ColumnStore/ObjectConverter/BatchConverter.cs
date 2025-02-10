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

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter
{
    public class BatchConverter
    {
        private readonly IReadOnlyList<ObjectConverterPropertyInfo> properties;
        private readonly IReadOnlyList<IObjectColumnConverter> converters;
        private readonly Func<object>? createObject;

        public BatchConverter(IReadOnlyList<ObjectConverterPropertyInfo> properties, IReadOnlyList<IObjectColumnConverter> converters, Func<object>? createObject)
        {
            this.properties = properties;
            this.converters = converters;
            this.createObject = createObject;
        }

        public void Serialize(object obj, IColumn[] columns)
        {
            for (int i = 0; i < properties.Count; i++)
            {
                var property = properties[i];

                if (property.GetFunc == null)
                {
                    throw new InvalidOperationException("Cannot serialize object without a get function");
                }

                var value = property.GetFunc(obj);

                if (value != null)
                {
                    var func = new AddToColumnFunc(columns[i]);
                    converters[i].Serialize(value, ref func);
                }
                else
                {
                    columns[i].Add(NullValue.Instance);
                }
            }
        }

        public object Deserialize(IColumn[] columns, int index)
        {
            if (createObject == null)
            {
                throw new InvalidOperationException("Cannot deserialize object without a create function");
            }
            var obj = createObject();
            for (int i = 0; i < properties.Count; i++)
            {
                var property = properties[i];
                var value = converters[i].Deserialize(columns[i].GetValueAt(index, default));

                if (property.SetFunc == null)
                {
                    throw new InvalidOperationException("Cannot deserialize object without a set function");
                }

                property.SetFunc(obj, value);
            }
            return obj;
        }

        public static BatchConverter GetBatchConverter(Type objectType, List<string> columnNames, ObjectConverterResolver? resolver = null)
        {
            if (resolver == null)
            {
                resolver = ObjectConverterResolver.Default;
            }
            var typeInfo = ObjectConverterTypeInfoLookup.GetTypeInfo(objectType);

            List<IObjectColumnConverter> converters = new List<IObjectColumnConverter>();
            List<ObjectConverterPropertyInfo> propertyInfos = new List<ObjectConverterPropertyInfo>();


            for (int i = 0; i < columnNames.Count; i++)
            {
                var columnName = columnNames[i];
                var property = typeInfo.Properties.FirstOrDefault(p => p.Name!.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                if (property == null)
                {
                    throw new InvalidOperationException($"Property {columnName} not found on object type {objectType}");
                }
                converters.Add(resolver.GetConverter(property.TypeInfo));
                propertyInfos.Add(property);
            }

            return new BatchConverter(propertyInfos, converters, typeInfo.CreateObject);
        }
    }
}
