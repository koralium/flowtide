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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AcceptanceTests
{
    public class ComparisonFunctionTests : FlowtideAcceptanceBase
    {
        [Fact]
        public async Task CoalesceTwoArgs()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT coalesce(nullablestring, firstName) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.NullableString != null ? x.NullableString : x.FirstName }));
        }

        [Fact]
        public async Task CoalesceOneArg()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT coalesce(nullablestring) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.NullableString }));
        }

        [Fact]
        public async Task IsInfinitePositiveInfiniteTrue()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(1 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = true }));
        }

        [Fact]
        public async Task IsInfiniteNegativeInfiniteTrue()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(-1 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = true }));
        }

        [Fact]
        public async Task IsInfiniteNaNFalse()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(0 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = false }));
        }

        [Fact]
        public async Task IsInfiniteStringFalse()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(firstName) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = false }));
        }
    }
}
