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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;

namespace FlowtideDotNet.AcceptanceTests
{
    public class PlanHashTests
    {
        private static void SeedUsers(FlowtideTestStream stream)
        {
            for (int i = 0; i < 10; i++)
            {
                stream.AddOrUpdateUser(new User() { UserKey = i, CompanyId = "1" });
            }
        }

        // A stream with stored state refuses another plan hash
        [Fact]
        public async Task HintsDoNotChangeThePlanHash()
        {
            var fileProvider = new KeepAliveMemoryFileProvider();
            await using (var before = new SharedStorageTestStream("PlanHash/Hints", fileProvider))
            {
                SeedUsers(before);
                await before.StartStream("INSERT INTO output SELECT userkey FROM users", version: "v1");
                await before.WaitForUpdate();
                await before.StopStream();
            }

            await using var after = new SharedStorageTestStream("PlanHash/Hints", fileProvider);
            SeedUsers(after);
            await after.StartStream("INSERT INTO output SELECT userkey FROM users WITH (WATERMARK_OUTPUT_MODE = AFTER_ALL_DATA)", version: "v1");
            after.AddOrUpdateUser(new User() { UserKey = 100, CompanyId = "1" });
            await after.WaitForUpdate();

            after.AssertCurrentDataEqual(after.Users.Select(x => new { x.UserKey }));
        }
    }
}
