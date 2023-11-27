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

using FlexBuffers;
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core
{
    public struct JoinedRowData : IRowData
    {
        private readonly int _leftLength;
        private readonly int _length;
        private readonly IRowData _left;
        private readonly IRowData _right;
        private readonly IReadOnlyList<int>? _emitList;

        public JoinedRowData(IRowData left, IRowData right, IReadOnlyList<int>? emitList)
        {
            _leftLength = left.Length;
            _length = left.Length + right.Length;
            _left = left;
            _right = right;
            _emitList = emitList;
        }

        public int Length
        {
            get
            {
                if (_emitList != null)
                {
                    return _emitList.Count;
                }
                return _length;
            }
        }



        public FlxValue GetColumn(int index)
        {
            if (_emitList != null)
            {
                index = _emitList[index];
            }
            if (index < _leftLength)
            {
                return _left.GetColumn(index);
            }
            return _right.GetColumn(index - _leftLength);
        }

        public FlxValueRef GetColumnRef(scoped in int index)
        {
            var mappedIndex = index;
            if (_emitList != null)
            {
                mappedIndex = _emitList[index];
            }
            if (mappedIndex < _leftLength)
            {
                return _left.GetColumnRef(mappedIndex);
            }
            int rightIndex = mappedIndex - _leftLength;
            return _right.GetColumnRef(rightIndex);
        }
    }
}
