﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Core.Operators.Write
{
    internal struct GroupedStreamEvent : IRowEvent
    {
        /// <summary>
        /// Target id is used during comparisons to be able to compare different events to each other
        /// </summary>
        private readonly byte _targetId;
        private readonly IRowData rowData;

        public GroupedStreamEvent(byte targetId, IRowData rowData)
        {
            _targetId = targetId;
            this.rowData = rowData;
        }

        public byte TargetId => _targetId;

        public IRowData RowData => rowData;

        public int Weight => 0;

        public uint Iteration => 0;

        public int Length => rowData.Length;

        public FlxValue GetColumn(int index)
        {
            return rowData.GetColumn(index);
        }

        public FlxValueRef GetColumnRef(in int index)
        {
            return rowData.GetColumnRef(index);
        }
    }
}
