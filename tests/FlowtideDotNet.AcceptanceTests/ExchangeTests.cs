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
    public class ExchangeTests : FlowtideAcceptanceBase
    {
        public ExchangeTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }


        [Fact]
        public async Task BroadcastExchangeTest()
        {
            GenerateData();

            await StartStream(@"
            CREATE VIEW dataview WITH (DISTRIBUTED = true) AS
            SELECT userkey FROM users;

            INSERT INTO outputtable
            SELECT * FROM dataview
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task ScatterExchangeTest()
        {
            GenerateData();

            await StartStream(@"
            CREATE VIEW dataview WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT=2) AS
            SELECT userkey FROM users;

            CREATE VIEW data_partition0 AS
            SELECT * FROM dataview WITH (PARTITION_ID = 0);

            CREATE VIEW data_partition1 AS
            SELECT * FROM dataview WITH (PARTITION_ID = 1);

            INSERT INTO outputtable
            SELECT 
            * 
            FROM data_partition0
            UNION
            SELECT 
            * 
            FROM data_partition1
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }
    }
}
