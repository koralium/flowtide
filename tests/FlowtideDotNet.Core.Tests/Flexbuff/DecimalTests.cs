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

using FlexBuffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.Flexbuff
{
    public class DecimalTests
    {
        [Fact]
        public void TestDecimal()
        {
            decimal expected = 12345678.91011M;
            var bytes = FlexBuffer.SingleValue(expected);
            var actual = FlxValue.FromMemory(bytes).AsDecimal;
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestDecimalInVector()
        {
            decimal expected = 12345678.91011M;
            var bytes = FlexBufferBuilder.Vector(v =>
            {
                v.Add(expected);
            });
            var actual = FlxValue.FromMemory(bytes).AsVector[0].AsDecimal;
            var actualRef = FlxValue.FromMemory(bytes).AsVector.GetRef(0).AsDecimal;
            Assert.Equal(expected, actual);
            Assert.Equal(expected, actualRef);
        }

        [Fact]
        public void TestAddDecimalFlxValue()
        {
            decimal expected = 12345678.91011M;
            var decimalBytes = FlexBuffer.SingleValue(expected);
            var flxVal = FlxValue.FromMemory(decimalBytes);
            var bytes = FlexBufferBuilder.Vector(v =>
            {
                v.Add(flxVal);
            });
            var actual = FlxValue.FromMemory(bytes).AsVector[0].AsDecimal;
            var actualRef = FlxValue.FromMemory(bytes).AsVector.GetRef(0).AsDecimal;
            Assert.Equal(expected, actual);
            Assert.Equal(expected, actualRef);
        }
    }
}
