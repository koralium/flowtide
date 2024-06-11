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
    public class StringColumn : IDataColumn
    {
        private byte[] _data = new byte[0];
        private int _length = 0;
        private List<int> _offsets = new List<int>();
        public int CompareToStrict(in int index, in IDataValue value)
        {
            var str = value.AsString;
            var dataSpan = _data.AsSpan();
            var startOffset = _offsets[index];
            if (index + 1 < _offsets.Count)
            {
                var endOffset = _offsets[index + 1];
                var length = endOffset - startOffset;
                return str.Span.SequenceCompareTo(dataSpan.Slice(startOffset, length));
            }
            else
            {
                var length = _length - startOffset;
                return str.CompareTo(new Flexbuffer.FlxString(dataSpan.Slice(startOffset, length)));
            }
        }

        public int CompareToStrict<T>(in int index, in T value)
            where T : IDataValue
        {
            var str = value.AsString;
            var dataSpan = _data.AsSpan();
            var startOffset = _offsets[index];
            if (index + 1 < _offsets.Count)
            {
                var endOffset = _offsets[index + 1];
                var length = endOffset - startOffset;
                return str.Span.SequenceCompareTo(dataSpan.Slice(startOffset, length));
            }
            else
            {
                var length = _length - startOffset;
                return str.CompareTo(new Flexbuffer.FlxString(dataSpan.Slice(startOffset, length)));
            }
        }
            

        private void EnsureCapacity(int length)
        {
            if (_data.Length < length)
            {
                var newData = new byte[length * 2];
                _data.CopyTo(newData, 0);
                _data = newData;
            }
        }

        public int Add<T>(in T value)
            where T: struct, IDataValue
        {
            var str = value.AsString;
            EnsureCapacity(_length + str.Span.Length);

            str.Span.CopyTo(_data.AsSpan(_length));
            var resultOffset = _offsets.Count;
            _offsets.Add(_length);
            _length += str.Span.Length;
            return resultOffset;
        }

        public int CompareToStrict(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is StringColumn stringColumn)
            {
                var str = new Flexbuffer.FlxString(_data.AsSpan(_offsets[thisIndex], _offsets[thisIndex + 1] - _offsets[thisIndex]));
                var otherStr = new Flexbuffer.FlxString(stringColumn._data.AsSpan(stringColumn._offsets[otherIndex], stringColumn._offsets[otherIndex + 1] - stringColumn._offsets[otherIndex]));
                return str.CompareTo(otherStr);
            }
            throw new NotImplementedException();
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            dataValueContainer._type = ArrowTypeId.String;
            dataValueContainer._stringValue = new StringValue(_data, _offsets[index], _offsets[index + 1]);
        }

        public IDataValue GetValueAt(in int index)
        {
            if (index + 1 < _offsets.Count)
            {
                // Boxing is expected here, as we are returning a reference type
                // Get value should only be used if its not possible to use the value directly from the array.
#pragma warning disable HAA0502 // Explicit new reference type allocation
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
                return new StringValue(_data, _offsets[index], _offsets[index + 1]);
            }
            else
            {
                return new StringValue(_data, _offsets[index], _length);
#pragma warning restore HAA0502 // Explicit new reference type allocation
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
            }
        }

        public int Update<T>(in int index, in T value)
            where T: struct, IDataValue
        {
            throw new NotImplementedException();
        }

        public int Add(in IDataValue value)
        {
            var str = value.AsString;
            EnsureCapacity(_length + str.Span.Length);

            str.Span.CopyTo(_data.AsSpan(_length));
            var resultOffset = _offsets.Count;
            _offsets.Add(_length);
            _length += str.Span.Length;
            return resultOffset;
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
    }
}
