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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Utils
{
    internal class SnapshotIdGenerator
    {
        public static long GenerateSnapshotId()
        {
            var guid = Guid.NewGuid();
            byte[] bytes = guid.ToByteArray();

            // Convert .NET Guid → Java UUID layout
            // .NET stores first three components in little-endian
            byte[] javaBytes = new byte[16];

            // time_low (4 bytes)
            javaBytes[0] = bytes[3];
            javaBytes[1] = bytes[2];
            javaBytes[2] = bytes[1];
            javaBytes[3] = bytes[0];

            // time_mid (2 bytes)
            javaBytes[4] = bytes[5];
            javaBytes[5] = bytes[4];

            // time_high_and_version (2 bytes)
            javaBytes[6] = bytes[7];
            javaBytes[7] = bytes[6];

            // clock_seq_hi_and_reserved + clock_seq_low + node (8 bytes)
            Buffer.BlockCopy(bytes, 8, javaBytes, 8, 8);

            // Now javaBytes matches Java UUID byte order
            long msb = BitConverter.ToInt64(javaBytes, 0);
            long lsb = BitConverter.ToInt64(javaBytes, 8);

            long xor32 = ((long)msb >> 32) ^ (uint)lsb;

            return (long)(xor32 & long.MaxValue);
        }
    }
}
