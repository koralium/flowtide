﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.AcceptanceTests
{
    public class JoinTests : FlowtideAcceptanceBase
    {
        [Fact]
        public async Task InnerJoinMergeJoin()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Join(Users, x => x.UserKey, x => x.UserKey, (l, r) => new { l.OrderKey, r.FirstName, r.LastName }));
        }

        [Fact]
        public async Task LeftJoinMergeJoin()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                LEFT JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders 
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order.OrderKey,
                    subuser.FirstName,
                    subuser.LastName
                });
        }

        [Fact]
        public async Task InnerJoinConditionAlwaysTrue()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON 1 = 1");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on 1 equals 1
                select new
                {
                    order.OrderKey,
                    user.FirstName,
                    user.LastName
                });
        }

        [Fact]
        public async Task CrossJoin()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                CROSS JOIN users u");
            await WaitForUpdate();
        }
    }
}