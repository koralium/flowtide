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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class MapColumn : IDataColumn
    {
        /// <summary>
        /// Contains all the property keys, must always be strings
        /// </summary>
        private StringColumn _keyColumn;

        /// <summary>
        /// Contains the values, can be any type
        /// </summary>
        private Column _valueColumn;

        private List<int> _offsets;

        public int Count => _offsets.Count;

        public ArrowTypeId Type => ArrowTypeId.Map;

        public MapColumn()
        {
            _keyColumn = new StringColumn();
            _valueColumn = new Column();
            _offsets = new List<int>();
        }

        private (int, int) GetOffsets(in int index)
        {
            var startOffset = _offsets[index];
            if ((index + 1)  >= _offsets.Count)
            {
                return (startOffset, _valueColumn.Count);
            }
            else
            {
                return (startOffset, _offsets[index + 1]);
            }
        }

        public IEnumerable<KeyValuePair<string, IDataValue>> GetKeyValuePairs(int index)
        {
            var (startOffset, endOffset) = GetOffsets(in index);

            for (int i = startOffset; i < endOffset; i++)
            {
                var key = _keyColumn.GetValueAt(i).AsString;
                var value = _valueColumn.GetValueAt(i);
                yield return new KeyValuePair<string, IDataValue>(Encoding.UTF8.GetString(key.Span), value);
            }
        }

        public int CompareToStrict(in int index, in IDataValue value)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index)
        {
            return new ReferenceMapValue(this, index);
        }

        public int Update(in int index, in IDataValue value)
        {
            return Add(value);
        }

        public int CompareTo<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var map = value.AsMap;
            // Sort keys so its possible to binary search after a key.
            // In future, can check if it is a reference map value or not to skip sorting
            var ordered = map.OrderBy(x => x.Key).ToList();
            var startOffset = _offsets.Count;
            _offsets.Add(_valueColumn.Count);
            foreach (var pair in ordered)
            {
                _keyColumn.Add(new StringValue(pair.Key));
                _valueColumn.Add(pair.Value);
            }

            return startOffset;
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            throw new NotImplementedException();
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int BinarySearch(in IDataValue dataValue)
        {
            throw new NotImplementedException();
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            throw new NotImplementedException();
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end) 
            where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public void RemoveAt(in int index)
        {
            throw new NotImplementedException();
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }
    }
}
