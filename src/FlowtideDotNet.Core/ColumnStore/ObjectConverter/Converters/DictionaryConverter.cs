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
    internal class DictionaryConverter<TKey, TValue> : IObjectColumnConverter
    {
        private readonly ObjectConverterTypeInfo typeInfo;
        private readonly IObjectColumnConverter keyConverter;
        private readonly IObjectColumnConverter valueConverter;

        public DictionaryConverter(ObjectConverterTypeInfo typeInfo, IObjectColumnConverter keyConverter, IObjectColumnConverter valueConverter)
        {
            this.typeInfo = typeInfo;
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
        }

        public object Deserialize<T>(T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                return null!;
            }

            if (value.Type == ArrowTypeId.Map)
            {
                var mapValue = value.AsMap;
                var dictionary = (IDictionary<TKey, TValue>)typeInfo.CreateObject!();
                foreach (var kv in mapValue)
                {
                    var key = keyConverter.Deserialize(kv.Key);
                    var innerVal = valueConverter.Deserialize(kv.Value);
                    dictionary.Add((TKey)key, (TValue)innerVal);
                }
                return dictionary;
            }

            throw new NotImplementedException();
        }

        public void Serialize(object obj, ref AddToColumnFunc addFunc)
        {
            if (obj == null)
            {
                addFunc.AddValue(NullValue.Instance);
            }
            if (obj is IDictionary<TKey, TValue> dictionary)
            {
                List<KeyValuePair<IDataValue, IDataValue>> values = new List<KeyValuePair<IDataValue, IDataValue>>();
                foreach (var kv in dictionary)
                {
                    var keyFunc = new AddToColumnFunc();
                    var valueFunc = new AddToColumnFunc();
                    keyConverter.Serialize(kv.Key!, ref keyFunc);
                    valueConverter.Serialize(kv.Value!, ref valueFunc);
                    values.Add(new KeyValuePair<IDataValue, IDataValue>(keyFunc.BoxedValue!, valueFunc.BoxedValue!));
                }
                addFunc.AddValue(new MapValue(values));
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
