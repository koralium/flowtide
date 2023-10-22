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

using System.Buffers;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal struct ByteMemoryOwner : IMemoryOwner<byte>
    {
        private byte[]? bytes;

        public ByteMemoryOwner(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public Memory<byte> Memory
        {
            get
            {
                Debug.Assert(bytes != null, "Memory already disposed");
                return bytes;
            }
        }

        public void Dispose()
        {
            bytes = null;
        }
    }
}
