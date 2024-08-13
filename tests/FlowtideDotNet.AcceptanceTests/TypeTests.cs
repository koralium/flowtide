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

using FlowtideDotNet.Substrait.Exceptions;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class TypeTests : FlowtideAcceptanceBase
    {
        public TypeTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TestGuid()
        {
            GenerateData();
            var order = Orders[0];
            await StartStream(@"
            INSERT INTO output
            SELECT
            GuidVal
            FROM orders
            WHERE guidval = guid('" + order.GuidVal.ToString() + @"')
            ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Where(x => x.GuidVal.Equals(order.GuidVal)).Select(x => new { x.GuidVal } ));
        }

        [Fact]
        public async Task TestDecimal()
        {
            GenerateData();
            var order = Orders[0];
            await StartStream(@"
            INSERT INTO output
            SELECT
            Money
            FROM orders
            WHERE money < 500
            ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Where(x => x.Money < 500).Select(x => new { x.Money }));

        }

        /// <summary>
        /// This test case covers a typical error that was made before type validation.
        /// Users would use sql server bit values 0 and 1 in boolean comparisons.
        /// This test makes sure that an error is thrown if equality is checked between two missmatching types.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestEqualityDifferentTypes()
        {
            GenerateData();
            var ex = await Assert.ThrowsAsync<SubstraitParseException>(async () =>
            {
                await StartStream(@"
                INSERT INTO output
                SELECT
                userkey
                FROM users
                WHERE active = 1
                ");
            });

            Assert.Equal("Missmatch type in equality: 'active = 1', type(Bool) = type(Int64)", ex.Message);
        }
    }
}
