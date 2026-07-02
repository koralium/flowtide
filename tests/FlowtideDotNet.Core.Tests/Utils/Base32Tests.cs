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

using FlowtideDotNet.Base.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.Utils
{
    public class Base32Tests
    {
        [Theory]
        [InlineData(new byte[] { }, "")]
        [InlineData(new byte[] { 72 }, "JA")]
        [InlineData(new byte[] { 72, 101 }, "JBSQ")]
        [InlineData(new byte[] { 1, 2, 3 }, "AEBAG")]
        public void EncodeBytes(byte[] input, string expected)
        {
            var result = Base32.Encode(input);
            Assert.Equal(expected, result);
        }

        /// <summary>
        /// Used to check with online decoders that the encoding is correct
        /// </summary>
        /// <param name="input"></param>
        /// <param name="expected"></param>
        [Theory]
        [InlineData("hello", "NBSWY3DP")]
        [InlineData("world", "O5XXE3DE")]
        public void EncodeStringWords(string input, string expected)
        {
            var result = Base32.Encode(Encoding.ASCII.GetBytes(input));
            Assert.Equal(expected, result);
        }
    }
}
