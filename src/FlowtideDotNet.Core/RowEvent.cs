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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Flexbuffer;
using System.Buffers;

namespace FlowtideDotNet.Core
{
    /// <summary>
    /// Basic row event
    /// </summary>
    public struct RowEvent : IRowEvent
    {
        private int _weight;
        private readonly uint _iteration;
        private readonly IRowData _rowData;

        public RowEvent(int weight, uint iteration, IRowData rowData)
        {
            _weight = weight;
            _iteration = iteration;
            _rowData = rowData;
        }

        public int Weight
        {
            get
            {
                return _weight;
            }
            set
            {
                _weight = value;
            }
        }

        public uint Iteration => _iteration;

        public int Length
        {
            get
            {
                return _rowData.Length;
            }
        }

        public IRowData RowData => _rowData;

        public FlxValue GetColumn(int index)
        {
            return _rowData.GetColumn(index);
        }

        public FlxValueRef GetColumnRef(in int index)
        {
            return _rowData.GetColumnRef(index);
        }

        public string ToJson()
        {
            var c = Compact(new FlexBuffer(ArrayPool<byte>.Shared));

            if (c._rowData is CompactRowData cc)
            {
                return cc.ToJson();
            }
            return string.Empty;
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

            for (int i = 0; i < _rowData.Length; i++)
            {
                flexBuffer.Add(_rowData.GetColumn(i));
            }
            flexBuffer.EndVector(vectorStart, false, false);
            var mem = flexBuffer.Finish();
            return new RowEvent(_weight, _iteration, new CompactRowData(mem, FlxValue.FromMemory(mem).AsVector));
        }

        public static RowEvent Create(int weight, uint iteration, Action<IFlexBufferVectorBuilder> vector)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared);
            buffer.NewObject();
            var start = buffer.StartVector();
            var builder = new FlexBufferVectorBuilder(buffer);
            vector(builder);
            buffer.EndVector(start, false, false);
            var fin = buffer.Finish();

            return new RowEvent(weight, iteration, new CompactRowData(fin, FlxValue.FromMemory(fin).AsVector));
        }

        public static int Compare(IRowEvent a, IRowEvent b)
        {
            for (int i = 0; i < a.Length; i++)
            {
                int compareResult = StrictFlxValueRefComparer.CompareTo(a.GetColumnRef(i), b.GetColumnRef(i));
                if (compareResult != 0)
                {
                    return compareResult;
                }
            }
            return 0;
        }
    }
}
