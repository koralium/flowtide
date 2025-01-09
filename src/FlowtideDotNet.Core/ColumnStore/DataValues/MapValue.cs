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
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct MapValue : IMapValue
    {
        private readonly IReadOnlyList<KeyValuePair<IDataValue, IDataValue>> keyValuePairs;

        public MapValue(IEnumerable<KeyValuePair<IDataValue, IDataValue>> keyValuePairs)
        {
            var arr = keyValuePairs.ToArray();
            Array.Sort(arr, new KeyValuePairDataValueComparer());
            this.keyValuePairs = arr;
        }

        public MapValue(params KeyValuePair<IDataValue, IDataValue>[] values)
        {
            Array.Sort(values, new KeyValuePairDataValueComparer());
            keyValuePairs = values;
        }

        public ArrowTypeId Type => ArrowTypeId.Map;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public Span<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => this;

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            for (int i = 0; i < keyValuePairs.Count; i++)
            {
                keyValuePairs[i].Key.AddToHash(hashAlgorithm);
                keyValuePairs[i].Value.AddToHash(hashAlgorithm);
            }
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Map;
            container._mapValue = this;
        }

        public IEnumerator<KeyValuePair<IDataValue, IDataValue>> GetEnumerator()
        {
            return keyValuePairs.GetEnumerator();
        }

        public void GetKeyAt(in int index, DataValueContainer result)
        {
            keyValuePairs[index].Key.CopyToContainer(result);
        }

        public IDataValue GetKeyAt(in int index)
        {
            return keyValuePairs[index].Key;
        }

        public int GetLength()
        {
            return keyValuePairs.Count;
        }

        public void GetValueAt(in int index, DataValueContainer result)
        {
            keyValuePairs[index].Value.CopyToContainer(result);
        }

        public IDataValue GetValueAt(in int index)
        {
            return keyValuePairs[index].Value;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return keyValuePairs.GetEnumerator();
        }
    }
}
