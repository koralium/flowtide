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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core
{
    public struct CompactRowData : IRowData
    {
        private readonly Memory<byte> _memory;
        private readonly FlxVector _vector;

        public CompactRowData(Memory<byte> memory, FlxVector vector)
        {
            _memory = memory;
            _vector = vector;
        }

        public CompactRowData(Memory<byte> memory)
        {
            _memory = memory;
            _vector = FlxValue.FromMemory(memory).AsVector;
        }

        public Span<byte> Span => _memory.Span;

        public int Length => _vector.Length;

        internal string ToJson()
        {
            return _vector.ToJson;
        }

        public FlxValue GetColumn(int index)
        {
            return _vector.Get(index);
        }

        public FlxValueRef GetColumnRef(scoped in int index)
        {
            return _vector.GetRef(index);
        }
    }
}
