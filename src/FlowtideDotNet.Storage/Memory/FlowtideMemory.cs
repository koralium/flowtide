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

namespace FlowtideDotNet.Storage.Memory
{
    /// <summary>
    /// A 64 byte aligned block of unmanaged memory.
    /// </summary>
    public readonly unsafe struct FlowtideMemory
    {
        /// <summary>
        /// The raw pointer to the start of the block.
        /// </summary>
        public readonly void* Pointer;

        /// <summary>
        /// The allocated size, can be larger than requested.
        /// </summary>
        public readonly int Length;

        internal FlowtideMemory(void* ptr, int length)
        {
            Pointer = ptr;
            Length = length;
        }

        /// <summary>
        /// True when no memory is attached.
        /// </summary>
        public bool IsNull => Pointer == null;

        /// <summary>
        /// A span over the whole block.
        /// </summary>
        public Span<byte> Span => new Span<byte>(Pointer, Length);
    }
}
