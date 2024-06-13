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

namespace FlowtideDotNet.Core.ColumnStore.Utils
{

    internal struct BinaryInfo
    {
        public readonly byte[] data;
        public readonly int index;
        public readonly int length;
        public BinaryInfo(byte[] data, int index, int length)
        {
            this.data = data;
            this.index = index;
            this.length = length;
        }
    }

    /// <summary>
    /// Helper list that stores binary data and their offsets.
    /// This follows apache arrow on how to store binary data.
    /// This means that it does not store references to the binary data, but instead stores them directly in the array.
    /// This list allows inserting data and removing data where it correctly recalculates offsets.
    /// </summary>
    internal class BinaryList
    {
        private byte[] _data;
        private IntList _offsets;
        private int _length;

        public int Count => _offsets.Count;

        public BinaryList()
        {
            _offsets = new IntList();
            _data = Array.Empty<byte>();
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

        /// <summary>
        /// Add binary data as an element to the list.
        /// </summary>
        /// <param name="data"></param>
        public void Add(Span<byte> data)
        {
            var currentOffset = _length;
            EnsureCapacity(_length + data.Length);
            data.CopyTo(_data.AsSpan(_length));
            _length += data.Length;
            _offsets.Add(currentOffset);
        }

        /// <summary>
        /// Insert binary data at a specfic index.
        /// If this is not inserted at the end, a copy will be done of all elements larger than this index.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="data"></param>
        public void Insert(int index, Span<byte> data)
        {
            if (index == _offsets.Count)
            {
                Add(data);
                return;
            }

            EnsureCapacity(_length + data.Length);
            // Get the offset of the current element at the location
            var offset = _offsets.Get(index);

            // Take out the length that all bytes must be moved
            var toMove =  data.Length;
            var span = _data.AsSpan();

            // Move all elements after the index
            span.Slice(offset, _length - offset).CopyTo(span.Slice(offset + toMove));
            
            // Insert data of the new element
            data.CopyTo(span.Slice(offset));

            // Add the offset and add the size to all offsets above this one.
            _offsets.InsertAt(index, offset, toMove);
            
            _length += data.Length;
        }

        public void RemoveAt(int index)
        {
            // Check if are removing the last element
            if (index == _offsets.Count - 1)
            {
                var offset = _offsets.Get(index);
                var length = _length - offset;
                _offsets.RemoveAt(index);
                _length -= length;
                return;
            }
            else
            {
                var offset = _offsets.Get(index);
                var length = _offsets.Get(index + 1) - offset;
                // Remove the offset and negate the length of all elements above this index.
                _offsets.RemoveAt(index, -length);
                
                var span = _data.AsSpan();

                // Move all elements after the index
                span.Slice(offset + length, _length - offset - length).CopyTo(span.Slice(offset));
                _length -= length;
            }
        }

        public Span<byte> Get(in int index)
        {
            var offset = _offsets.Get(index);
            if (index == _offsets.Count - 1)
            {
                return _data.AsSpan(offset, _length - offset);
            }
            else
            {
                return _data.AsSpan(offset, _offsets.Get(index + 1) - offset);
            }
        }

        /// <summary>
        /// Returns the underlying information, the raw array and index and offset.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public BinaryInfo GetBinaryInfo(in int index)
        {
            var offset = _offsets.Get(index);
            if (index == _offsets.Count - 1)
            {
                return new BinaryInfo(_data, offset, _length - offset);
            }
            else
            {
                return new BinaryInfo(_data, offset, _offsets.Get(index + 1) - offset);
            }
        }
    }
}
