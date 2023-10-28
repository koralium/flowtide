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
    public class ArithmaticFunctionTests : FlowtideAcceptanceBase
    {
        [Fact]
        public async Task SelectWithAdditionNumber()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey + 17 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey + 17 }));
        }

        [Fact]
        public async Task SelectWithAdditionString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName + 17 as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithSubtractionNumber()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey - 17 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey - 17 }));
        }

        [Fact]
        public async Task SelectWithSubtractionString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName - 17 as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }
    }
}
