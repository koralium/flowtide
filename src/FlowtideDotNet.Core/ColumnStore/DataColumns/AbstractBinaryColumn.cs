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
    public abstract class AbstractBinaryColumn : IDataColumn
    {
        private static byte[] s_emptyArray = new byte[0];
        protected byte[] _data;
        protected int _length = 0;
        protected List<int> _offsets = new List<int>();

        public AbstractBinaryColumn()
        {
            _data = s_emptyArray;
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

        protected int Add(in Span<byte> value)
        {
            EnsureCapacity(_length + value.Length);
            value.CopyTo(_data.AsSpan(_length));
            var resultOffset = _offsets.Count;
            _offsets.Add(_length);
            _length += value.Length;
            return resultOffset;
        }


        public abstract int Add(in IDataValue value);

        public abstract int CompareToStrict(in int index, in IDataValue value);

        public abstract int CompareToStrict(in IDataColumn otherColumn, in int thisIndex, in int otherIndex);

        public abstract IDataValue GetValueAt(in int index);

        public abstract int Update(in int index, in IDataValue value);

        public int CompareToStrict<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int Add<T>(in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }

        public abstract void GetValueAt(in int index, in DataValueContainer dataValueContainer);

        public int BinarySearch(in IDataValue dataValue)
        {
            throw new NotImplementedException();
        }

        public int BinarySearch(in IDataValue dataValue, int start, int end)
        {
            throw new NotImplementedException();
        }

        public (int, int) SearchBoundries(in IDataValue dataValue, int start, int end)
        {
            throw new NotImplementedException();
        }
    }
}
