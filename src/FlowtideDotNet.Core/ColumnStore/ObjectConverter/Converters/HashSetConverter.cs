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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using System.Collections;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters
{
    internal class HashSetConverter<TType> : IObjectColumnConverter
    {
        private readonly ObjectConverterTypeInfo typeInfo;
        private readonly IObjectColumnConverter innerConverter;

        public HashSetConverter(ObjectConverterTypeInfo typeInfo, IObjectColumnConverter innerConverter)
        {
            this.typeInfo = typeInfo;
            this.innerConverter = innerConverter;
        }

        public object Deserialize<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return null!;
            }

            if (value.Type == ArrowTypeId.List)
            {
                var newObj = (ICollection<TType>)typeInfo.CreateObject!();
                var listVal = value.AsList;

                for (int i = 0; i < listVal.Count; i++)
                {
                    newObj.Add((TType)innerConverter.Deserialize(listVal.GetAt(i)));
                }

                return newObj;
            }
            else
            {
                throw new NotImplementedException($"Cannot convert {value.Type} to HashSet");
            }
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            if (obj is IEnumerable enumerable)
            {
                List<IDataValue> values = new List<IDataValue>();

                foreach (var item in enumerable)
                {
                    AddToColumnFunc func = new AddToColumnFunc();
                    innerConverter.Serialize(item, ref func);
                    values.Add(func.BoxedValue!);
                }

                // Sort all the values to ensure that the hashset is always in the same order
                // This is required to produce deterministic results
                values.Sort(DataValueComparer.Instance);

                addFunc.AddValue(new ListValue(values));
                return;
            }
            throw new NotImplementedException();
        }
    }
}
