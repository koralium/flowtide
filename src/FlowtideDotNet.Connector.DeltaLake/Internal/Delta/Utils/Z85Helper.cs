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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils
{
    internal class Z85Helper
    {
        private const string encoder = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
        //static char[] encoder = {
        //    "0123456789"
        //    "abcdefghij"
        //    "klmnopqrst"
        //    "uvwxyzABCD"
        //    "EFGHIJKLMN"
        //    "OPQRSTUVWX"
        //    "YZ.-:+=^!/"
        //    "*?&<>()[]{"
        //    "}@%$#"
        //};


        static byte[] decoder = {
            0x00, 0x44, 0x00, 0x54, 0x53, 0x52, 0x48, 0x00,
            0x4B, 0x4C, 0x46, 0x41, 0x00, 0x3F, 0x3E, 0x45,
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x40, 0x00, 0x49, 0x42, 0x4A, 0x47,
            0x51, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A,
            0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32,
            0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A,
            0x3B, 0x3C, 0x3D, 0x4D, 0x00, 0x4E, 0x43, 0x00,
            0x00, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
            0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
            0x21, 0x22, 0x23, 0x4F, 0x00, 0x50, 0x00, 0x00
        };


        public static byte[] Decode(string str)
        {
            //  Accepts only strings bounded to 5 bytes
            if (str.Length % 5 != 0)
            {
                throw new ArgumentException("Z85 string length must be a multiple of 5.", nameof(str));
            }
            
            int decoded_size = str.Length * 4 / 5;
            //byte* decoded = malloc(decoded_size);

            byte[] decoded = new byte[decoded_size];

            uint byte_nbr = 0;
            int char_nbr = 0;
            uint value = 0;
            while (char_nbr < str.Length)
            {
                //  Accumulate value in base 85
                value = value * 85 + decoder[(byte)str[char_nbr++] - 32];
                if (char_nbr % 5 == 0)
                {
                    //  Output value in base 256
                    uint divisor = 256 * 256 * 256;
                    while (divisor != 0)
                    {
                        decoded[byte_nbr++] = (byte)(value / divisor % 256);
                        divisor /= 256;
                    }
                    value = 0;
                }
            }
            //assert(byte_nbr == decoded_size);
            return decoded;
        }

        public static Guid DecodeToGuid(string z85Str)
        {
            byte[] bigEndianBytes = Decode(z85Str);

            // Convert back to little-endian for Guid constructor
            byte[] littleEndianBytes = new byte[16];
            Array.Copy(bigEndianBytes, littleEndianBytes, 16);

            // Rearrange first three fields to little-endian
            Array.Reverse(littleEndianBytes, 0, 4);  // Time-low
            Array.Reverse(littleEndianBytes, 4, 2);  // Time-mid
            Array.Reverse(littleEndianBytes, 6, 2);  // Time-high-and-version

            return new Guid(littleEndianBytes);
        }

    }
}
