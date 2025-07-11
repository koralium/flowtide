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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils
{
    public static class Z85
    {
        private const string Z85Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
        private const int Z85Base = 85;

        public static string Encode(byte[] data)
        {
            if (data.Length % 4 != 0)
                throw new ArgumentException("Input byte array length must be a multiple of 4.", nameof(data));

            ulong value = 0;
            char[] result = new char[data.Length * 5 / 4];
            int resultIndex = 0;

            for (int i = 0; i < data.Length; i += 4)
            {
                value = (ulong)data[i] << 24 |
                        (ulong)data[i + 1] << 16 |
                        (ulong)data[i + 2] << 8 |
                        data[i + 3];

                for (int j = 4; j >= 0; j--)
                {
                    result[resultIndex + j] = Z85Chars[(int)(value % Z85Base)];
                    value /= Z85Base;
                }
                resultIndex += 5;
            }

            return new string(result);
        }

        public static byte[] Decode(string z85Str)
        {
            if (z85Str.Length % 5 != 0)
                throw new ArgumentException("Z85 string length must be a multiple of 5.", nameof(z85Str));

            int byteLength = z85Str.Length * 4 / 5;
            byte[] result = new byte[byteLength];
            ulong value = 0;
            int byteIndex = 0;

            for (int i = 0; i < z85Str.Length; i += 5)
            {
                value = 0;

                for (int j = 0; j < 5; j++)
                {
                    int charIndex = Z85Chars.IndexOf(z85Str[i + j]);
                    if (charIndex == -1)
                        throw new FormatException($"Invalid character '{z85Str[i + j]}' in Z85 string.");
                    value = value * Z85Base + (ulong)charIndex;
                }

                for (int j = 3; j >= 0; j--)
                {
                    result[byteIndex + j] = (byte)(value & 0xFF);
                    value >>= 8;
                }
                byteIndex += 4;
            }

            return result;
        }

        public static string EncodeGuid(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            // Convert to big-endian for Z85 encoding
            Array.Reverse(bytes, 0, 4);  // Time-low
            Array.Reverse(bytes, 4, 2);  // Time-mid
            Array.Reverse(bytes, 6, 2);  // Time-high-and-version

            return Encode(bytes);
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
