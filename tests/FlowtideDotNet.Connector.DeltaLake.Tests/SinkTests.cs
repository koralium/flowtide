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

using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Interfaces;
using DeltaLake.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class SinkTests
    {
        [Fact]
        public async Task TestInsertInitialData()
        {
            var engine = new DeltaEngine(EngineOptions.Default);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int64Type.Default, true))
                .Field(new Field("name", StringType.Default, true))
                .Build();
            var table = await engine.CreateTableAsync(new TableCreateOptions("memory://test", schema),default);
            var stream = new DeltaLakeTestStream(nameof(TestInsertInitialData), new List<string>() { "id" }, "memory://test", table);
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO test
            SELECT
                userkey as id,
                FirstName as name
            FROM users
            ");

            long? countNumber = 0;
            while (true)
            {
                await stream.SchedulerTick();
                await table.UpdateIncrementalAsync(default, default);
                var countQueryEnumerable = table.QueryAsync(new SelectQuery("SELECT count(*) FROM deltatable"), default);
                countNumber = ((await countQueryEnumerable.FirstAsync()).Column(0) as Int64Array)!.GetValue(0);
                if (countNumber > 0)
                {
                    break;
                }
            }
            Assert.Equal(1000, countNumber);
        }

        /// <summary>
        /// This test make sure the merge into works correctly for initial batch
        /// where existing data should be removed if it is not in the dataset.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestInsertInitialDataRemovesPrevious()
        {
            var engine = new DeltaEngine(EngineOptions.Default);
            var schema = new Schema.Builder()
                .Field(new Field("id", Int64Type.Default, true))
                .Field(new Field("name", StringType.Default, true))
                .Build();
            var table = await engine.CreateTableAsync(new TableCreateOptions("memory://test", schema), default);
            var stream = new DeltaLakeTestStream(nameof(TestInsertInitialData), new List<string>() { "id" }, "memory://test", table);
            stream.Generate(2000);

            var existingIds = new Int64Array.Builder()
                .Append(stream.Users[0].UserKey)
                .Append(999999)
                .Build();
            var existingNames = new StringArray.Builder()
                .Append(stream.Users[0].FirstName)
                .Append("John Doe")
                .Build();

            var existingArrays = new List<IArrowArray>() { existingIds, existingNames };
            var existingBatch = new RecordBatch(schema, existingArrays, 2);
            await table.InsertAsync(new List<RecordBatch>() { existingBatch }, schema, new InsertOptions() { SaveMode = SaveMode.Append }, default);

            await stream.StartStream(@"
            INSERT INTO test
            SELECT
                userkey as id,
                FirstName as name
            FROM users
            ");

            long? countNumber = 0;
            while (true)
            {
                await stream.SchedulerTick();
                await table.UpdateIncrementalAsync(default, default);
                var countQueryEnumerable = table.QueryAsync(new SelectQuery("SELECT count(*) FROM deltatable"), default);
                countNumber = ((await countQueryEnumerable.FirstAsync()).Column(0) as Int64Array)!.GetValue(0);
                if (countNumber == 2000)
                {
                    break;
                }
            }
            Assert.Equal(2000, countNumber);
        }
    }
}
