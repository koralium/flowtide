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

using FASTER.core;
using FlexBuffers;
using System.Buffers;

namespace FlowtideDotNet.Core.Operators.Read
{
    public struct IngressData : ILogEnqueueEntry
    {
        private FlxVector _vector;
        internal bool IsDeleted { get; set; }

        internal IngressData(byte[] bytes, bool isDeleted)
        {
            Memory = bytes;
            _vector = FlxValue.FromMemory(bytes).AsVector;
            IsDeleted = isDeleted;
        }

        public Span<byte> Span => Memory;

        public byte[] Memory { get; internal set; }

        public FlxVector Vector => _vector;

        public int SerializedLength => Memory.Length + 1;

        public void SerializeTo(Span<byte> dest)
        {
            dest[0] = IsDeleted ? (byte)1 : (byte)0;
            Span.CopyTo(dest.Slice(1));
        }

        public static IngressData Create(Action<IFlexBufferVectorBuilder> vector)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared);
            buffer.NewObject();
            var start = buffer.StartVector();
            var builder = new FlexBufferVectorBuilder(buffer);
            vector(builder);
            buffer.EndVector(start, false, false);
            var fin = buffer.Finish();
            return new IngressData(fin, false);
        }


    }
}
