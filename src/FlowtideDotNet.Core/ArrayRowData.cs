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

namespace FlowtideDotNet.Core
{
    public struct ArrayRowData : IRowData
    {
        private readonly FlxValue[] valueArray;

        public ArrayRowData(FlxValue[] valueArray)
        {
            this.valueArray = valueArray;
        }

        public static ArrayRowData Create(IRowData row, IReadOnlyList<int>? emitList)
        {
            FlxValue[]? arr = null;
            if (emitList != null)
            {
                arr = new FlxValue[emitList.Count];
                for (int i = 0; i < emitList.Count; i++)
                {
                    var index = emitList[i];

                    arr[i] = row.GetColumn(index);
                }
            }
            else
            {
                arr = new FlxValue[row.Length];
                for (int i = 0; i < row.Length; i++)
                {
                    arr[i] = row.GetColumn(i);
                }
            }
            return new ArrayRowData(arr);
        }

        public static ArrayRowData Create(IRowData left, IRowData right, IReadOnlyList<int>? emitList)
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
            return new ArrayRowData(arr);
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
        }

        public FlxValueRef GetColumnRef(scoped in int index)
        {
            return valueArray[index].GetRef();
        }
    }
}
