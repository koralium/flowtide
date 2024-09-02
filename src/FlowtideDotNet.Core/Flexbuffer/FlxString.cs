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

using System.Text;

namespace FlowtideDotNet.Core.Flexbuffer
{
    public ref struct FlxString
    {
        private readonly Span<byte> span;

        public Span<byte> Span => span;

        public FlxString(Span<byte> span)
        {
            this.span = span;
        }

        public int CompareTo(in FlxString other)
        {
            return Compare(this, other);
        }

        public static int Compare(in FlxString v1, in FlxString v2)
        {
            return v1.span.SequenceCompareTo(v2.span);
        }

        public static int CompareIgnoreCase(in FlxString v1, in FlxString v2)
        {
            return Utf8Utility.CompareToOrdinalIgnoreCaseUtf8(v1.span, v2.span);
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(span);
        }
    }
}
