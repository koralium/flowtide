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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using Stowage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeSinkTests
    {

        [Fact]
        public async Task TestCreateTable()
        {
            var storage = Files.Of.LocalDisk("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(TestCreateTable), storage);

            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    userkey INT,
                    Name STRING,
                    LastName STRING,
                    NullableString STRING
                );

                INSERT INTO test
                SELECT userKey, firstName as Name, lastName, NullableString FROM users
            ");

            await WaitForVersion(storage, "test", stream, 0);

            var firstUser = stream.Users[0];
            stream.DeleteUser(firstUser);

            stream.Generate(50);

            await WaitForVersion(storage, "test", stream, 1);

            firstUser = stream.Users.Last();
            stream.DeleteUser(firstUser);

            await WaitForVersion(storage, "test", stream, 2);
        }


        /// <summary>
        /// Check that null is added to columns that are not present in the select statement
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestInsertSelectionOfColumns()
        {
            var storage = Files.Of.LocalDisk("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(TestCreateTable), storage);

            var fields = new List<StructField>()
            {
                 new StructField("userkey", new IntegerType(), true, new Dictionary<string, object>()),
                 new StructField("name", new StringType(), true, new Dictionary<string, object>()),
                 new StructField("LastName", new StringType(), true, new Dictionary<string, object>()),
                 new StructField("NullableString", new StringType(), true, new Dictionary<string, object>())
            };
            var schemaStruct = new StructType(fields);

            JsonSerializerOptions jsonOptions = new JsonSerializerOptions();
            jsonOptions.Converters.Add(new TypeConverter());

            await DeltaTransactionWriter.WriteCommit(storage, "test2", 0, new List<Internal.Delta.Actions.DeltaAction>()
            {
                new Internal.Delta.Actions.DeltaAction()
                {
                    MetaData = new Internal.Delta.Actions.DeltaMetadataAction()
                    {
                        SchemaString = JsonSerializer.Serialize(schemaStruct as SchemaBaseType, jsonOptions)
                    }
                }
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO test2
                SELECT firstName as Name, NullableString FROM users
            ");

            await WaitForVersion(storage, "test2", stream, 1);
        }

        [Fact]
        public async Task TestInsertIntoStruct()
        {
            var storage = Files.Of.LocalDisk("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(TestCreateTable), storage);

            var fields = new List<StructField>()
            {
                 new StructField("userkey", new IntegerType(), true, new Dictionary<string, object>()),
                 new StructField("Struct", new StructType(new List<StructField>()
                 {
                     new StructField("name", new StringType(), true, new Dictionary<string, object>()),
                     new StructField("lastName", new StringType(), true, new Dictionary<string, object>())
                 }), true, new Dictionary<string, object>())
            };
            var schemaStruct = new StructType(fields);

            JsonSerializerOptions jsonOptions = new JsonSerializerOptions();
            jsonOptions.Converters.Add(new TypeConverter());

            await DeltaTransactionWriter.WriteCommit(storage, "test3", 0, new List<Internal.Delta.Actions.DeltaAction>()
            {
                new Internal.Delta.Actions.DeltaAction()
                {
                    MetaData = new Internal.Delta.Actions.DeltaMetadataAction()
                    {
                        SchemaString = JsonSerializer.Serialize(schemaStruct as SchemaBaseType, jsonOptions)
                    }
                }
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO test3
                SELECT userkey, map('name', firstName, 'lastName', lastName) as struct FROM users
            ");

            await WaitForVersion(storage, "test3", stream, 1);
        }

        private async Task WaitForVersion(IFileStorage storage, string tableName, FlowtideTestStream stream, long version)
        {
            while (true)
            {
                try
                {
                    var exists = await storage.Exists($"/{tableName}/_delta_log/{version.ToString("D20")}.json");
                    if (exists)
                    {
                        break;
                    }
                }
                catch (Exception)
                {
                }
                
                await stream.SchedulerTick();
                await Task.Delay(100);
            }
        }
    }
}
