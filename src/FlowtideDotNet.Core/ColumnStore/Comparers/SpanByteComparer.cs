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

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    internal class SpanByteComparer : ISpanByteComparer
    {
        public static readonly SpanByteComparer Instance = new SpanByteComparer();
        public int Compare(in ReadOnlySpan<byte> x, in ReadOnlySpan<byte> y)
        {
            return x.SequenceCompareTo(y);
        }
    }

    internal class SpanByteComparerDesc : ISpanByteComparer
    {
        public static readonly SpanByteComparerDesc Instance = new SpanByteComparerDesc();
        public int Compare(in ReadOnlySpan<byte> x, in ReadOnlySpan<byte> y)
        {
            return y.SequenceCompareTo(x);
        }
    }
}
