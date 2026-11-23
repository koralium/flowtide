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
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.GenericDataTests
{
    public class GenericMixedDeltaTests
    {
        [Fact]
        public async Task ADeleteBeforeUpsertsInOneDeltaKeepsEachRowWithItsKey()
        {
            var source = new TestDataSource(default);
            source.AddChange(new FlowtideGenericObject<User>("1", new User { UserKey = 1, FirstName = "A" }, 1, false));
            source.AddChange(new FlowtideGenericObject<User>("2", new User { UserKey = 2, FirstName = "B" }, 1, false));

            var stream = new GenericDataTestStream<User>(source, "GenericMixedDelta");
            stream.RegisterTableProviders(builder =>
            {
                builder.AddGenericDataTable<User>("users");
            });

            await stream.StartStream(@"
                INSERT INTO output
                SELECT
                    UserKey,
                    FirstName
                FROM users
            ");
            await stream.WaitForUpdate();

            source.AddChange(new FlowtideGenericObject<User>("1", null, 2, true));
            source.AddChange(new FlowtideGenericObject<User>("2", new User { UserKey = 2, FirstName = "B2" }, 3, false));
            source.AddChange(new FlowtideGenericObject<User>("3", new User { UserKey = 3, FirstName = "C" }, 4, false));
            await stream.Trigger("delta_load");
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new List<User>()
            {
                new User { UserKey = 2, FirstName = "B2" },
                new User { UserKey = 3, FirstName = "C" }
            }.Select(x => new { x.UserKey, x.FirstName }));
        }
    }
}
