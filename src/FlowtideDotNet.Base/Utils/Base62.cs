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
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base.Utils
{
    internal static class Base62
    {
        private const string Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        private static readonly BigInteger Radix = new BigInteger(62);

        public static string Encode(ReadOnlySpan<byte> data)
        {
            if (data == null || data.Length == 0) return string.Empty;

            // Interpret input as an unsigned big-endian integer
            BigInteger intData = new BigInteger(data, isUnsigned: true, isBigEndian: true);

            if (intData.IsZero)
            {
                int leadingZeros = data.Length;
                return new string('0', leadingZeros);
            }
            
            var sb = new StringBuilder();
            while (intData > 0)
            {
                int remainder = (int)(intData % Radix);
                intData /= Radix;
                sb.Insert(0, Alphabet[remainder]);
            }

            // Preserve leading zero bytes in the input as '0' characters
            int leadingZeroBytes = CountLeadingZeros(data);
            if (leadingZeroBytes > 0)
                sb.Insert(0, new string('0', leadingZeroBytes));

            return sb.ToString();
        }

        private static int CountLeadingZeros(ReadOnlySpan<byte> data)
        {
            int count = 0;
            foreach (var b in data)
            {
                if (b == 0)
                    count++;
                else
                    break;
            }
            return count;
        }
    }
}
