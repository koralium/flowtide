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

/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using FASTER.core;
using FlexBuffers;
using FlowtideDotNet.Core.Flexbuffer;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core
{
    public struct StreamEvent : IRowEvent
    {
        private readonly Memory<byte> _data;
        private int _weight;
        private FlxVector _vector;
        private readonly uint _iteration;

        public StreamEvent(int weight, uint iteration, Memory<byte> bytes)
        {
            _data = bytes;
            _weight = weight;
            _vector = FlxValue.FromMemory(bytes).AsVector;
            _iteration = iteration;
        }

        internal StreamEvent(int weight, uint iteration, Memory<byte> bytes, FlxVector vector)
        {
            _data = bytes;
            _weight = weight;
            _vector = vector;
            _iteration = iteration;
        }

        public int Length => _vector.Length;

        //public Span<byte> Span => _data.Span;

        //public Memory<byte> Memory => _data;

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

        //public FlxVector Vector => _vector;

        public int SerializedLength => _data.Length + 4;

        //public void SerializeTo(Span<byte> dest)
        //{
        //    BinaryPrimitives.WriteInt32LittleEndian(dest, _weight);
        //    Span.CopyTo(dest.Slice(4));
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FlxValue GetColumn(int index)
        {
            return _vector[index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FlxValueRef GetColumnRef(in int index)
        {
            return _vector.GetRef(index);
        }

        public static StreamEvent CreateFromWal(Memory<byte> bytes)
        {
            var span = bytes.Span;
            var weight = BinaryPrimitives.ReadInt32LittleEndian(span);
            return new StreamEvent(weight, 0, bytes.Slice(4));
        }

        public static StreamEvent Create(int weight, uint iteration, Action<IFlexBufferVectorBuilder> vector)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared);
            buffer.NewObject();
            var start = buffer.StartVector();
            var builder = new FlexBufferVectorBuilder(buffer);
            vector(builder);
            buffer.EndVector(start, false, false);
            var fin = buffer.Finish();
            return new StreamEvent(weight, iteration, fin);
        }

        
    }
}
