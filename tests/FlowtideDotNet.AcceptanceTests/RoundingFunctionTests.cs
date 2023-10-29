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
    public class RoundingFunctionTests : FlowtideAcceptanceBase
    {
        [Fact]
        public async Task CeilOnNumeric()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT ceil(userkey / 3) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (long)Math.Ceiling(x.UserKey / (double)3) }));
        }

        [Fact]
        public async Task CeilingOnNumeric()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT ceiling(userkey / 3) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (long)Math.Ceiling(x.UserKey / (double)3) }));
        }

        [Fact]
        public async Task CeilingOnString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT ceiling(firstName) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = default(string) }));
        }

        [Fact]
        public async Task FloorOnNumeric()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT floor(userkey / 3) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (long)Math.Floor(x.UserKey / (double)3) }));
        }

        [Fact]
        public async Task FloorOnString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT floor(firstName) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = default(string) }));
        }

        [Fact]
        public async Task RoundOnNumeric()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT round(userkey / 3) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (long)Math.Round(x.UserKey / (double)3) }));
        }

        [Fact]
        public async Task RoundOnString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT round(firstName) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = default(string) }));
        }
    }
}
