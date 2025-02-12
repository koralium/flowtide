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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;

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

        public EventBatchData ConvertToEventBatch(IEnumerable<object> objects, IMemoryAllocator memoryAllocator)
        {
            var columns = new IColumn[properties.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = new Column(memoryAllocator);
            }

            foreach (var obj in objects)
            {
                AppendToColumns(obj, columns);
            }

            return new EventBatchData(columns);
        }

        /// <summary>
        /// Special function used in unit tests that retuns the rows in sorted order to easily compare with expected results
        /// 
        /// This method is not optimized at all and reuses code and does alot of memory copy to reuse components, so dont use it for other things than unit tests.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objects"></param>
        /// <param name="memoryAllocator"></param>
        /// <returns></returns>
        public static EventBatchData ConvertToBatchSorted<T>(IEnumerable<T> objects, IMemoryAllocator memoryAllocator)
        {
            var converter = GetBatchConverter(typeof(T));

            // Reuse column key storage contaienr and its comparer to find the insert indices
            var container = new ColumnKeyStorageContainer(converter.properties.Count, memoryAllocator);
            var columnComparer = new ColumnComparer(converter.properties.Count);
            var data = converter.ConvertToEventBatch(objects, memoryAllocator);

            for (int i = 0; i < data.Count; i++)
            {
                var rowRef = new ColumnRowReference() { referenceBatch = data, RowIndex = i };
                var insertIndex = columnComparer.FindIndex(rowRef, container);

                if (insertIndex < 0)
                {
                    insertIndex = ~insertIndex;
                }

                container.Insert(insertIndex, rowRef);
            }

            // Dispose the data
            data.Dispose();

            // Create a new data with the sorted rows and copy the data over to them
            Column[] columns = new Column[container._data.Columns.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = container._data.Columns[i].Copy(memoryAllocator);
            }
            // Dispose the container
            container.Dispose();

            return new EventBatchData(columns);
        }

        public EventBatchData ConvertToEventBatch<T>(IEnumerable<T> objects, IMemoryAllocator memoryAllocator)
        {
            var columns = new IColumn[properties.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = new Column(memoryAllocator);
            }

            foreach (var obj in objects)
            {
                AppendToColumns(obj, columns);
            }

            return new EventBatchData(columns);
        }

        public EventBatchData ConvertToEventBatch(IMemoryAllocator memoryAllocator, params object[] objects)
        {
            return ConvertToEventBatch(objects, memoryAllocator);
        }

        public void AppendToColumns(object obj, IColumn[] columns)
        {
            if (columns.Length != properties.Count)
            {
                throw new InvalidOperationException($"Input column count '{columns.Length}' does not match property count '{properties.Count}'");
            }
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

        public IEnumerable<object> ConvertToDotNetObjects(EventBatchData data)
        {
            if (createObject == null)
            {
                throw new InvalidOperationException("Cannot deserialize object without a create function");
            }
            for (int i = 0; i < data.Count; i++)
            {
                yield return ConvertToDotNetObject(data.Columns, i);
            }
        }

        public object ConvertToDotNetObject(IReadOnlyList<IColumn> columns, int index)
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

        public static BatchConverter GetBatchConverter(Type objectType, List<string>? columnNames = null, ObjectConverterResolver? resolver = null)
        {
            if (resolver == null)
            {
                resolver = ObjectConverterResolver.Default;
            }
            var typeInfo = ObjectConverterTypeInfoLookup.GetTypeInfo(objectType);

            List<IObjectColumnConverter> converters = new List<IObjectColumnConverter>();
            List<ObjectConverterPropertyInfo> propertyInfos = new List<ObjectConverterPropertyInfo>();

            if (columnNames == null)
            {
                columnNames = new List<string>();
                foreach (var property in typeInfo.Properties)
                {
                    columnNames.Add(property.Name!);
                }
            }

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
