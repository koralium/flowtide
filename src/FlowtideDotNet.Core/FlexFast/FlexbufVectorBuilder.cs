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

namespace FlowtideDotNet.Core.FlexFast
{
    internal ref struct FlexbufVectorBuilder
    {
        private Stack<ulong> _stack;
        private Span<long> _spanStack;
        private Span<byte> _typesStack;
        private int _stackOffset;
        private ulong _maxSize;

        private ref struct Test
        {
            public long val;
        }

        private Span<byte> _buffer;
        private ref int _offset;

        public FlexbufVectorBuilder(Span<long> stack, Span<byte> buffer, Span<byte> typesStack, ref int bufferOffset)
        {
            _stack = new Stack<ulong>();
            _spanStack = stack;
            _stackOffset = 0;
            _buffer = buffer;
            _typesStack = typesStack;
            _offset = bufferOffset;
        }

        /// <summary>
        /// Write an integer
        /// </summary>
        /// <param name="value"></param>
        public void Add(int value)
        {
            _typesStack[_stackOffset] = (byte)FlexType.Int;
            _spanStack[_stackOffset] = value;
            _stackOffset++;
        }

        public static byte PackedType(FlexBuffers.Type type)
        {
            return (byte)((byte)type << 2);
        }

        public static byte PackedType(FlexBuffers.Type type, BitWidth bitWidth)
        {
            return (byte)((byte)bitWidth | ((byte)type << 2));
        }

        public void Finish()
        {
            // Check buffer size before writing
            for (int i = 0; i < _stackOffset; i++)
            {
                // Check if it is a inline type, then write the value directly.
                if (_typesStack[i] == (byte)FlexType.Int)
                {
                    //_buffer[_offset] = _stack.;
                }
            }
        }
    }
}
