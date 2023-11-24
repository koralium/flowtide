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
    /// <summary>
    /// Basic row event
    /// </summary>
    public struct RowEvent : IRowEvent
    {
        private readonly int _weight;
        private readonly uint _iteration;
        private readonly IRowData _rowData;
        private readonly IReadOnlyList<int>? _emitList;

        public RowEvent(int weight, uint iteration, IRowData rowData, IReadOnlyList<int>? emitList)
        {
            _weight = weight;
            _iteration = iteration;
            _rowData = rowData;
            _emitList = emitList;
        }
        public int Weight => _weight;

        public uint Iteration => _iteration;

        public int Length
        {
            get
            {
                if (_emitList != null)
                {
                    return _emitList.Count;
                }
                return _rowData.Length;
            }
        }

        public IRowData RowData => _rowData;

        public FlxValue GetColumn(int index)
        {
            if (_emitList != null)
            {
                return _rowData.GetColumn(_emitList[index]);
            }
            return _rowData.GetColumn(index);
        }

        public FlxValueRef GetColumnRef(in int index)
        {
            if (_emitList != null)
            {
                return _rowData.GetColumnRef(_emitList[index]);
            }
            return _rowData.GetColumnRef(index);
        }

        /// <summary>
        /// Returns a compact row event, is useful after many joins to reduce the recursive depth
        /// to locate values
        /// </summary>
        /// <param name="flexBuffer"></param>
        /// <returns></returns>
        public RowEvent Compact(FlexBuffer flexBuffer)
        {
            if (_rowData is CompactRowData)
            {
                return this;
            }
            flexBuffer.NewObject();
            var vectorStart = flexBuffer.StartVector();
            
            if (_emitList != null)
            {
                for (int i = 0; i < _emitList.Count; i++)
                {
                    flexBuffer.Add(_rowData.GetColumn(_emitList[i]));
                }
            }
            flexBuffer.EndVector(vectorStart, false, false);
            var mem = flexBuffer.Finish();
            return new RowEvent(_weight, _iteration, new CompactRowData(mem, FlxValue.FromMemory(mem).AsVector), default);
        }
    }
}
