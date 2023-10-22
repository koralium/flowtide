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
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;

namespace FlowtideDotNet.Core.FlexFast
{
    internal ref struct FlexbufBuilder
    {
        private readonly MemoryPool<byte> _pool;
        private IMemoryOwner<byte> _memory;
        private Span<byte> _buffer;
        private int _offset;

        public FlexbufBuilder(MemoryPool<byte> pool)
        {
            _pool = pool;
        }

        public delegate void OnVector(FlexbufVectorBuilder builder);

        public void Vector(OnVector onVector)
        {

        }

        public void Start(int estimatedSize)
        {
            _memory = _pool.Rent(estimatedSize);
            _buffer = _memory.Memory.Span;
            _offset = 0;
        }

        /// <summary>
        /// Start a vector of a certain size and with a bitwidth
        /// </summary>
        /// <param name="size"></param>
        /// <param name="bitWidth"></param>
        public void StartVector(int size, BitWidth bitWidth)
        {
            // Write the size
            WriteULong((ulong)size, bitWidth);
        }

        public BitWidth WriteBlob(Span<byte> span)
        {
            WriteULong((ulong)span.Length, BitWidth.Width8);
            CheckBuffer(span.Length);
            span.CopyTo(_buffer.Slice(_offset));
            _offset += span.Length;
            return BitWidth.Width8;
        }

        public unsafe BitWidth WriteString(string str)
        {
            CheckBuffer(str.Length * 2);
            var sizeLoc = _offset;
            //WriteULong((ulong)str.Length, BitWidth.Width8);
            // Get estimated bit width
            _offset += 1;
            fixed (char* s = str)
            {
                fixed (byte* buffer = &MemoryMarshal.GetReference(_buffer))
                {
                    int writtenSize = Encoding.UTF8.GetBytes(s, str.Length, buffer + _offset, _buffer.Length - _offset);
                    int size = (writtenSize + 1);
                    _offset += size;
                    _buffer[sizeLoc] = (byte)(size);
                }
            }

            return BitWidth.Width8;
        }

        private void WriteULong(ulong value, BitWidth bitWidth)
        {
            if (bitWidth == BitWidth.Width8)
            {
                CheckBuffer(1);
                WriteByte((byte)value);
                _offset++;
            }
            else if (bitWidth == BitWidth.Width16)
            {
                CheckBuffer(2);
                BinaryPrimitives.WriteUInt16LittleEndian(_buffer.Slice(_offset), (ushort)value);
                _offset += 2;
            }
            else if (bitWidth == BitWidth.Width32)
            {
                CheckBuffer(4);
                BinaryPrimitives.WriteUInt32LittleEndian(_buffer.Slice(_offset), (ushort)value);
                _offset += 4;
            }
            else if (bitWidth == BitWidth.Width64)
            {
                CheckBuffer(8);
                BinaryPrimitives.WriteUInt64LittleEndian(_buffer, value);
                _offset += 8;
            }
        }

        private void CheckBuffer(int newData)
        {
            if (_buffer.Length < (newData + _offset))
            {
                var oldMem = _memory;
                var newMem = _pool.Rent(_memory.Memory.Length);
                oldMem.Memory.CopyTo(newMem.Memory);
                _memory = newMem;
                _buffer = _memory.Memory.Span;
                oldMem.Dispose();
            }
        }

        private void WriteByte(byte val)
        {
            _buffer[_offset] = val;
            _offset++;
        }

        //private static BitWidth Width(int value)
        //{
        //    if (value >= 0)
        //    {
        //        if (value <= sbyte.MaxValue)
        //        {
        //            return BitWidth.Width8;
        //        }
        //        return value <= short.MaxValue ? BitWidth.Width16 : BitWidth.Width32;
        //    }
        //    if (value >= sbyte.MinValue)
        //    {
        //        return BitWidth.Width8;
        //    }
        //    return value >= short.MinValue ? BitWidth.Width16 : BitWidth.Width32;
        //}
    }
}
