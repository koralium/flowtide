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

namespace FlowtideDotNet.Core.Operators.Join
{

    public struct JoinStreamEvent
    {
        private readonly Memory<byte> _data;
        
        /// <summary>
        /// Target id is used during comparisons to be able to compare different events to each other
        /// </summary>
        private readonly byte _targetId;
        private readonly uint _iteration;

        private FlxVector _vector;

        public JoinStreamEvent(Memory<byte> data, uint iteration, byte targetId, FlxVector vector)
        {
            _data = data;
            _targetId = targetId;
            _vector = vector;
            _iteration = iteration;
        }

        /// <summary>
        /// 4 is added to keep it aligned * 4.
        /// </summary>
        public int SerializedLength => _data.Length + 4;

        public Span<byte> Span => _data.Span;

        public Memory<byte> Memory => _data;

        public FlxVector Vector => _vector;

        public byte TargetId => _targetId;

        public uint Iteration => _iteration;
    }
}
