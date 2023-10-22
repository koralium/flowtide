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

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal class Functions : SpanByteFunctions<long, byte[], long>
    {
        //private protected readonly MemoryPool<byte> memoryPool;

        ///// <summary>
        ///// Constructor
        ///// </summary>
        ///// <param name="memoryPool"></param>
        //public Functions(MemoryPool<byte> memoryPool = default)
        //{
        //    this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        //}

        public unsafe override bool SingleReader(ref long key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            //value.CopyTo(ref dst, memoryPool);
            return true;
        }

        /// <inheritdoc />
        public unsafe override bool ConcurrentReader(ref long key, ref SpanByte input, ref SpanByte value, ref byte[] dst, ref ReadInfo readInfo)
        {
            dst = value.ToByteArray();
            //value.CopyTo(ref dst, memoryPool);
            return true;
        }
    }
}
