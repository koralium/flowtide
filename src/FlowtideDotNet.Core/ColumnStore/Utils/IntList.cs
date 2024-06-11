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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    /// <summary>
    /// Special list data structure that stores integers only
    /// This data structure is useful when storing offsets for instance since it can change offset locations during removal.
    /// </summary>
    internal class IntList
    {
        private int[] _data;
        private int _length;

        public IntList()
        {
            _data = Array.Empty<int>();
        }

        public Span<int> Span => _data;

        public int Count => _length;

        private void EnsureCapacity(int length)
        {
            if (_data.Length < length)
            {
                var newData = new int[length * 2];
                _data.CopyTo(newData, 0);
                _data = newData;
            }
        }

        public void Add(int item)
        {
            EnsureCapacity(_length + 1);
            _data[_length++] = item;
        }

        public void RemoveAt(int index)
        {
            var span = _data.AsSpan();
            span.Slice(index + 1, _length - index - 1).CopyTo(span.Slice(index));
            _length--;
        }

        /// <summary>
        /// Special remove at, where it runs an addition on all elements that are larger than the removed index.
        /// This is useful if this is used to store offsets where all offsets can be moved during the copy.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="additionOnMoved"></param>
        public void RemoveAt(int index, int additionOnMoved)
        {
            var span = _data.AsSpan();
            var source = span.Slice(index + 1, _length - index - 1);
            var dest = span.Slice(index);
            AvxUtils.InPlaceMemCopyWithAddition(span, index + 1, index, _length - index - 1, additionOnMoved);
            //AvxUtils.MemCpyWithAdd(source, dest, additionOnMoved);
            _length--;
        }

        public void InsertAt(int index, int item)
        {
            EnsureCapacity(_length + 1);
            var span = _data.AsSpan();
            span.Slice(index, _length - index).CopyTo(span.Slice(index + 1));
            _data[index] = item;
            _length++;
        }

        public void InsertAt(int index, int item, int additionOnMoved)
        {
            EnsureCapacity(_length + 1);
            var span = _data.AsSpan();
            var source = span.Slice(index, _length - index);
            var dest = span.Slice(index + 1);
            AvxUtils.InPlaceMemCopyWithAddition(span, index, index + 1, _length - index, additionOnMoved);
            //AvxUtils.MemCpyWithAdd(source, dest, additionOnMoved);
            _data[index] = item;
            _length++;
        }

        public void Update(int index, int item)
        {
            _data[index] = item;
        }

        /// <summary>
        /// Special update operation, it allows doing an addition on all elements above this one.
        /// This is useful if this is used to store offsets where all offsets can be moved during the copy.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        /// <param name="additionOnAbove"></param>
        public void Update(int index, int item, int additionOnAbove)
        {
            _data[index] = item;
            AvxUtils.AddValueToElements(_data.AsSpan(index + 1, _length - index - 1), additionOnAbove);
        }

        public int Get(in int index)
        {
            return _data[index];
        }
    }
}
