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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class SetTests : FlowtideAcceptanceBase
    {
        public SetTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TestUnionAll()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { u.UserKey })).ToList());
        }

        [Fact]
        public async Task TestUnionDistinct()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT UserKey FROM users
            UNION DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey }).Union(Users.Select(u => new { u.UserKey })).ToList());
        }

        [Fact]
        public async Task TestExceptDistinct()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            EXCEPT DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            var expected = Users.Select(x => new { x.UserKey }).Union(Users.Select(u => new { UserKey = u.UserKey + 1 })).Except(Users.Select(x => new { x.UserKey })).Distinct().ToList();
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task TestExceptDistinctWithUpdate()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            EXCEPT DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            GenerateData();

            await WaitForUpdate();
            var expected = Users.Select(x => new { x.UserKey }).Union(Users.Select(u => new { UserKey = u.UserKey + 1 })).Except(Users.Select(x => new {x.UserKey})).Distinct().ToList();
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task TestExceptAll()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            EXCEPT ALL
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { UserKey = u.UserKey + 1 }));
        }

        [Fact]
        public async Task TestIntersectDistinct()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            INTERSECT DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey }));
        }

        [Fact]
        public async Task TestIntersectAll()
        {
            GenerateData();

            var firstUser = Users.First();

            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey + 1 FROM users;

            CREATE VIEW otherset AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey FROM users;            

            INSERT INTO output
            SELECT UserKey FROM test
            INTERSECT ALL
            SELECT UserKey FROM otherset");

            await WaitForUpdate();

            var unionOtherset = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { u.UserKey })).ToList();
            var unionTest = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { UserKey = u.UserKey + 1 })).ToList();

            var expected = Users.Take(0).Select(x => new { x.UserKey }).ToList();

            for (int i = 0; i < unionTest.Count; i++)
            {
                if (unionOtherset.Contains(unionTest[i]))
                {
                    expected.Add(unionTest[i]);
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task TestIntersectAllWithUpdate()
        {
            GenerateData();

            var firstUser = Users.First();

            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey + 1 FROM users;

            CREATE VIEW otherset AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey FROM users;            

            INSERT INTO output
            SELECT UserKey FROM test
            INTERSECT ALL
            SELECT UserKey FROM otherset");

            await WaitForUpdate();

            GenerateData();

            await WaitForUpdate();

            var unionOtherset = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { u.UserKey })).ToList();
            var unionTest = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { UserKey = u.UserKey + 1 })).ToList();

            var expected = Users.Take(0).Select(x => new { x.UserKey }).ToList();

            for (int i = 0; i < unionTest.Count; i++)
            {
                if (unionOtherset.Contains(unionTest[i]))
                {
                    expected.Add(unionTest[i]);
                }
            }

            AssertCurrentDataEqual(expected);
        }
    }
}
