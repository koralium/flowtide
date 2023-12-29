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

using System.Buffers.Binary;
using System.IO.Hashing;

namespace FlowtideDotNet.Core.Operators.Join
{
    internal class RowDataHasher
    {
        private XxHash64 xxHash;
        private byte[] _destination;
        public RowDataHasher()
        {
            xxHash = new XxHash64();
            _destination = new byte[8];
        }

        public ulong Hash(IRowData data)
        {
            for (int i = 0; i < data.Length; i++)
            {
                data.GetColumnRef(i).AddToHash(xxHash);
            }
            xxHash.GetHashAndReset(_destination);
            return BinaryPrimitives.ReadUInt64BigEndian(_destination);
        }
    }
}
