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
    internal class ArrayConverter : IObjectColumnConverter
    {
        private readonly ObjectConverterTypeInfo typeInfo;
        private readonly IObjectColumnConverter innerConverter;
        private readonly Type innerType;

        public ArrayConverter(ObjectConverterTypeInfo typeInfo, IObjectColumnConverter innerConverter, Type innerType)
        {
            this.typeInfo = typeInfo;
            this.innerConverter = innerConverter;
            this.innerType = innerType;
        }

        public object Deserialize<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return null!;
            }

            if (value.Type == ArrowTypeId.List)
            {
                var listVal = value.AsList;
                var listLength = listVal.Count;

                var obj = Array.CreateInstance(innerType, listLength);
                
                for (int i = 0; i < listLength; i++)
                {
                    var element = innerConverter.Deserialize(listVal.GetAt(i));
                    obj.SetValue(element, i);
                }

                return obj;
            }
            else
            {
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

            var arr = (Array)obj;

            var list = new List<IDataValue>();

            for (int i = 0; i < arr.Length; i++)
            {
                var func = new AddToColumnFunc();
                var element = arr.GetValue(i);
                if (element == null)
                {
                    list.Add(NullValue.Instance);
                }
                else
                {
                    innerConverter.Serialize(element, ref func);
                    list.Add(func.BoxedValue!);
                }
            }
            addFunc.AddValue(new ListValue(list));
        }
    }
}
