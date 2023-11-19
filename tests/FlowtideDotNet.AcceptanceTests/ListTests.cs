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
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class ListTests : FlowtideAcceptanceBase
    {
        public ListTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task ListAggWithGrouping()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, list_agg(orderkey)
                FROM orders
                GROUP BY userkey
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, MinVal = x.Select(y => y.OrderKey).ToList() }));
        }

        [Fact]
        public async Task ListAgg()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(orderkey)
                FROM orders
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new [] { new { list = Orders.Select(x => x.OrderKey).ToList() } });
        }

        [Fact]
        public async Task ListAggWithObject()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(map(orderkey, orderkey))
                FROM orders
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Orders.Select(x => new KeyValuePair<string, int>(x.OrderKey.ToString(), x.OrderKey)).ToList() } });
        }
    }
}
