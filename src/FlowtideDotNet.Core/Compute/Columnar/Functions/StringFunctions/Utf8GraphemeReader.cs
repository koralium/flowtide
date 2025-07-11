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
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StringFunctions
{
    internal ref struct Utf8GraphemeReader
    {
        private ReadOnlySpan<byte> _utf8;
        private int _position;

        public int CurrentStart { get; private set; }

        public Utf8GraphemeReader(ReadOnlySpan<byte> utf8)
        {
            _utf8 = utf8;
            _position = 0;
            CurrentStart = 0;
        }

        public bool MoveNext()
        {
            if (_position >= _utf8.Length)
                return false;

            CurrentStart = _position;

            // First rune
            var status = Rune.DecodeFromUtf8(_utf8.Slice(_position), out Rune rune, out int bytesConsumed);
            
            if (status != System.Buffers.OperationStatus.Done)
                throw new InvalidOperationException("Invalid UTF-8 sequence.");

            _position += bytesConsumed;

            // Consume following runes that are combining marks
            while (_position < _utf8.Length)
            {
                status = Rune.DecodeFromUtf8(_utf8.Slice(_position), out Rune nextRune, out int nextBytes);
                
                if (status != System.Buffers.OperationStatus.Done)
                    break;

                var cat = CharUnicodeInfo.GetUnicodeCategory(nextRune.Value);
                if (cat != UnicodeCategory.NonSpacingMark &&
                    cat != UnicodeCategory.SpacingCombiningMark &&
                    cat != UnicodeCategory.EnclosingMark)
                {
                    break;
                }

                _position += nextBytes;
            }

            return true;
        }
    }

}
