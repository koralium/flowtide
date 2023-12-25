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
        private readonly FlxValue[] valueArray;

        public JoinedRowData(FlxValue[] valueArray)
        {
            this.valueArray = valueArray;
        }

        public static JoinedRowData Create(IRowData left, IRowData right, IReadOnlyList<int>? emitList)
        {
            var leftLength = left.Length;
            FlxValue[]? arr = null;
            if (emitList != null)
            {
                arr = new FlxValue[emitList.Count];
                for (int i = 0; i < emitList.Count; i++)
                {
                    var index= emitList[i];

                    if (index < leftLength)
                    {
                        arr[i] = left.GetColumn(index);
                    }
                    else
                    {
                        arr[i] = right.GetColumn(index - leftLength);
                    }
                }
            }
            else
            {
                arr = new FlxValue[left.Length + right.Length];
                for (int i = 0; i < left.Length; i++)
                {
                    arr[i] = left.GetColumn(i);
                }
                for (int i = 0; i < right.Length; i++)
                {
                    arr[i + left.Length] = right.GetColumn(i);
                }
            }
            return new JoinedRowData(arr);
        }

        public int Length
        {
            get
            {
                return valueArray.Length;
            }
        }



        public FlxValue GetColumn(int index)
        {
            return valueArray[index];
            //if (_emitList != null)
            //{
            //    index = _emitList[index];
            //}
            //if (index < _leftLength)
            //{
            //    return _left.GetColumn(index);
            //}
            //return _right.GetColumn(index - _leftLength);
        }

        public FlxValueRef GetColumnRef(scoped in int index)
        {
            return valueArray[index].GetRef();
            //var mappedIndex = index;
            //if (_emitList != null)
            //{
            //    mappedIndex = _emitList[index];
            //}
            //if (mappedIndex < _leftLength)
            //{
            //    return _left.GetColumnRef(mappedIndex);
            //}
            //int rightIndex = mappedIndex - _leftLength;
            //return _right.GetColumnRef(rightIndex);
        }
    }
}
