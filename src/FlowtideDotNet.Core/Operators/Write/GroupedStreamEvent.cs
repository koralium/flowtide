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

namespace FlowtideDotNet.Core.Operators.Write
{
    internal struct GroupedStreamEvent
    {
        private readonly Memory<byte> _data;

        /// <summary>
        /// Target id is used during comparisons to be able to compare different events to each other
        /// </summary>
        private readonly byte _targetId;

        private FlxVector _vector;

        public GroupedStreamEvent(Memory<byte> data, byte targetId, FlxVector vector)
        {
            _data = data;
            _targetId = targetId;
            _vector = vector;
        }

        /// <summary>
        /// 4 is added to keep it aligned * 4.
        /// </summary>
        public int SerializedLength => _data.Length + 4;

        public Span<byte> Span => _data.Span;

        public Memory<byte> Memory => _data;

        public FlxVector Vector => _vector;

        public byte TargetId => _targetId;

        public void SerializeTo(Span<byte> dest)
        {
            dest[0] = _targetId;
            _data.Span.CopyTo(dest.Slice(4));
        }

        public static GroupedStreamEvent CreateFromMemory(Memory<byte> data)
        {
            var span = data.Span;
            var targetId = span[0];
            var vector = FlxValue.FromMemory(data.Slice(4)).AsVector;
            return new GroupedStreamEvent(data.Slice(4), targetId, vector);
        }
    }
}
