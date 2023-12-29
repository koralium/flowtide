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

using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace FlowtideDotNet.Core.Tests
{
    public class HashTests
    {
        [Fact]
        public void TestCompileHashFunction()
        {
            var testEvent = RowEvent.Create(1, 0, b =>
            {
                b.Add("testval");
            });

            XxHash32 xxhash = new XxHash32();
            testEvent.GetColumnRef(0).AddToHash(xxhash);
            var dest = new byte[4];
            xxhash.GetHashAndReset(dest);
            var expected = BinaryPrimitives.ReadUInt32BigEndian(dest);

            var expressions = new List<Substrait.Expressions.Expression>()
            {
                new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = 0
                    }
                }
            };
            var reg = new FunctionsRegister();
            var function = HashCompiler.CompileGetHashCode(expressions, reg);

            
            var hash = function(testEvent);
            Assert.Equal(expected, hash);
        }
    }
}
