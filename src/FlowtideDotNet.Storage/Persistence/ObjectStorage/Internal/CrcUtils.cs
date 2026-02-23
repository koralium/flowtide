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

using FlowtideDotNet.Storage.Exceptions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal class CrcUtils
    {
        public static void CheckPageCrc32(Crc32 checker, long pageId, ReadOnlySequence<byte> data, uint crc)
        {
            checker.Reset();
            var pos = data.Start;
            while (data.TryGet(ref pos, out var memory))
            {
                checker.Append(memory.Span);
            }
            var actual = checker.GetCurrentHashAsUInt32();
            if (actual != crc)
            {
                throw new FlowtideChecksumMismatchException($"Invalid CRC32 for page '{pageId}'");
            }
        }

        public static void CheckCrc32(ReadOnlySpan<byte> data, uint crc)
        {
            var actualcrc = Crc32.HashToUInt32(data);
            if (actualcrc != crc)
            {
                throw new FlowtideChecksumMismatchException($"Invalid CRC32 for page.");
            }
        }

        public static void CheckCrc64(ulong expected, ReadOnlySequence<byte> data)
        {
            var crcCalculator = new Crc64();
            foreach (var segment in data)
            {
                crcCalculator.Append(segment.Span);
            }

            var actualcrc = crcCalculator.GetCurrentHashAsUInt64();
            if (actualcrc != expected)
            {
                throw new FlowtideChecksumMismatchException($"Invalid CRC64. Expected: {expected}, Actual: {actualcrc}");
            }
        }
    }
}
