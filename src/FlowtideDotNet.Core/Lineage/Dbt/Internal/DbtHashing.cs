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

using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    internal static class DbtHashing
    {
        public static string Sha256Hex(ReadOnlySpan<byte> data)
        {
            return Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();
        }

        public static string Sha256Hex(string text)
        {
            return Sha256Hex(Encoding.UTF8.GetBytes(text));
        }

        public static string Hash8(params string[] parts)
        {
            return Sha256Hex(string.Join('\u001f', parts)).Substring(0, 8);
        }

        // RFC 9562 version 8 layout over the content hash.
        public static Guid DeterministicUuid(ReadOnlySpan<byte> data)
        {
            Span<byte> bytes = stackalloc byte[16];
            SHA256.HashData(data).AsSpan(0, 16).CopyTo(bytes);
            bytes[6] = (byte)((bytes[6] & 0x0F) | 0x80);
            bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
            return new Guid(bytes, bigEndian: true);
        }

        public static string ETag(ReadOnlySpan<byte> data)
        {
            return "\"" + Sha256Hex(data) + "\"";
        }
    }
}
