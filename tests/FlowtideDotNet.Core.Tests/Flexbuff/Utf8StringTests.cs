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

using FlowtideDotNet.Core.Flexbuffer;
using System.Text;

namespace FlowtideDotNet.Core.Tests.Flexbuff
{
    public class Utf8StringTests
    {
        [Fact]
        public void TestLowercaseStringEqualsCompare()
        {
            var val = "testa";
            var bytes = Encoding.UTF8.GetBytes(val);

            ReadOnlySpan<byte> span = new Span<byte>(bytes);

            var compareToVal = span.CompareToOrdinalIgnoreCaseUtf8(span);
            Assert.Equal(0, compareToVal);
        }

        [Fact]
        public void TestCaseDifferentStringEqualsCompare()
        {
            var val = "testa";
            var bytes1 = Encoding.UTF8.GetBytes(val);
            var bytes2 = Encoding.UTF8.GetBytes("Testa");

            ReadOnlySpan<byte> span = new Span<byte>(bytes1);

            var compareToVal = span.CompareToOrdinalIgnoreCaseUtf8(bytes2);
            Assert.Equal(0, compareToVal);
        }

        [Fact]
        public void TestLeftLongerThanRight()
        {
            var bytes1 = Encoding.UTF8.GetBytes("testa1");
            var bytes2 = Encoding.UTF8.GetBytes("testa");

            ReadOnlySpan<byte> span = new Span<byte>(bytes1);

            var compareToVal = span.CompareToOrdinalIgnoreCaseUtf8(bytes2);
            Assert.Equal(1, compareToVal);
        }

        [Fact]
        public void TestRightLongerThanLeft()
        {
            var bytes1 = Encoding.UTF8.GetBytes("testa");
            var bytes2 = Encoding.UTF8.GetBytes("testa1");

            ReadOnlySpan<byte> span = new Span<byte>(bytes1);

            var compareToVal = span.CompareToOrdinalIgnoreCaseUtf8(bytes2);
            Assert.Equal(-1, compareToVal);
        }
    }
}
