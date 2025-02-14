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
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct ReferenceMapValue : IMapValue
    {
        internal readonly MapColumn mapColumn;
        internal readonly int index;

        public ReferenceMapValue(MapColumn mapColumn, int index)
        {
            this.mapColumn = mapColumn;
            this.index = index;
        }

        public ArrowTypeId Type => ArrowTypeId.Map;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public ReadOnlySpan<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => this;

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => throw new NotImplementedException();

        public void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm)
        {
            var length = GetLength();
            for (int i = 0; i < length; i++)
            {
                var key = GetKeyAt(i);
                var value = GetValueAt(i);
                key.AddToHash(hashAlgorithm);
                value.AddToHash(hashAlgorithm);
            }
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._type = ArrowTypeId.Map;
            container._mapValue = this;
        }

        public IEnumerator<KeyValuePair<IDataValue, IDataValue>> GetEnumerator()
        {
            return mapColumn.GetKeyValuePairs(index).GetEnumerator();
        }

        public void GetKeyAt(in int index, DataValueContainer result)
        {
            mapColumn.GetKeyAt(this.index, index, result);
        }

        public IDataValue GetKeyAt(in int index)
        {
            return mapColumn.GetKeyAt(this.index, index);
        }

        public int GetLength()
        {
            return mapColumn.GetElementLength(index);
        }

        public void GetValueAt(in int index, DataValueContainer result)
        {
            mapColumn.GetMapValueAt(this.index, index, result);
        }

        public IDataValue GetValueAt(in int index)
        {
            return mapColumn.GetMapValueAt(this.index, index);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return mapColumn.GetKeyValuePairs(index).GetEnumerator();
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("{");
            var first = true;
            foreach (var kv in this)
            {
                if (!first)
                {
                    sb.Append(", ");
                }
                first = false;
                sb.Append(kv.Key);
                sb.Append(": ");
                sb.Append(kv.Value);
            }
            sb.Append("}");
            return sb.ToString();
        }
    }
}
