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
            var storage = Files.Of.InternalMemory("./test");
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
            
            await AssertResult(nameof(TestCreateTable), storage, "test", 3, stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName, x.NullableString }));
        }


        /// <summary>
        /// Check that null is added to columns that are not present in the select statement
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestInsertSelectionOfColumns()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(TestInsertSelectionOfColumns), storage);

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

            // Wait 2 commits, first one is just the table creation
            await AssertResult(nameof(TestInsertSelectionOfColumns), storage, "test2", 2, stream.Users.Select(x => new { UserKey = default(int?), Name = x.FirstName, LastName = default(string?), x.NullableString }));
        }

        [Fact]
        public async Task WriteStruct()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteStruct), storage);

            await CreateInitialCommitWithSchema(storage, "test3", new List<StructField>()
            {
                 new StructField("userkey", new IntegerType(), true, new Dictionary<string, object>()),
                 new StructField("Struct", new StructType(new List<StructField>()
                 {
                     new StructField("name", new StringType(), true, new Dictionary<string, object>()),
                     new StructField("lastName", new StringType(), true, new Dictionary<string, object>())
                 }), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO test3
                SELECT userkey, map('name', firstName, 'lastName', lastName) as struct FROM users
            ");

            await WaitForVersion(storage, "test3", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "test3", stream, 2);

            await AssertResult(nameof(WriteStruct), storage, "test3", 3, stream.Users.Select(x => new { x.UserKey, Struct = new { name = x.FirstName, lastName = x.LastName } }));
        }

        [Fact]
        public async Task WriteInt8()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteInt8), storage);

            await CreateInitialCommitWithSchema(storage, "int8test", new List<StructField>()
            {
                 new StructField("userkey", new ByteType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO int8test
                SELECT userkey % 100 as userkey FROM users
            ");

            await WaitForVersion(storage, "int8test", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "int8test", stream, 2);

            await AssertResult(nameof(WriteInt8), storage, "int8test", 3, stream.Users.Select(x => new { userkey = x.UserKey % 100 }));
        }

        [Fact]
        public async Task WriteInt16()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteInt16), storage);

            await CreateInitialCommitWithSchema(storage, "int16test", new List<StructField>()
            {
                 new StructField("userkey", new ShortType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO int16test
                SELECT userkey FROM users
            ");

            await WaitForVersion(storage, "int16test", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "int16test", stream, 2);

            await AssertResult(nameof(WriteInt16), storage, "int16test", 3, stream.Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task WriteInt32()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteInt32), storage);

            await CreateInitialCommitWithSchema(storage, "int32test", new List<StructField>()
            {
                 new StructField("userkey", new IntegerType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO int32test
                SELECT userkey FROM users
            ");

            await WaitForVersion(storage, "int32test", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "int32test", stream, 2);

            await AssertResult(nameof(WriteInt32), storage, "int32test", 3, stream.Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task WriteInt64()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteInt64), storage);

            await CreateInitialCommitWithSchema(storage, "int64test", new List<StructField>()
            {
                 new StructField("userkey", new LongType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO int64test
                SELECT userkey FROM users
            ");

            await WaitForVersion(storage, "int64test", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "int64test", stream, 2);

            await AssertResult(nameof(WriteInt64), storage, "int64test", 3, stream.Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task WriteFloat32()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteFloat32), storage);

            await CreateInitialCommitWithSchema(storage, "float32test", new List<StructField>()
            {
                 new StructField("DoubleValue", new FloatType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO float32test
                SELECT DoubleValue FROM users
            ");

            await WaitForVersion(storage, "float32test", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "float32test", stream, 2);

            await AssertResult(nameof(WriteFloat32), storage, "float32test", 3, stream.Users.Select(x => new { DoubleValue = (float)x.DoubleValue }));
        }

        [Fact]
        public async Task WriteFloat64()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteFloat64), storage);

            await CreateInitialCommitWithSchema(storage, "float64test", new List<StructField>()
            {
                 new StructField("DoubleValue", new DoubleType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO float64test
                SELECT DoubleValue FROM users
            ");

            await WaitForVersion(storage, "float64test", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "float64test", stream, 2);

            await AssertResult(nameof(WriteFloat64), storage, "float64test", 3, stream.Users.Select(x => new { DoubleValue = x.DoubleValue }));
        }

        [Fact]
        public async Task WriteBoolean()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteBoolean), storage);

            await CreateInitialCommitWithSchema(storage, "booltest", new List<StructField>()
            {
                 new StructField("Active", new BooleanType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO booltest
                SELECT Active FROM users
            ");

            await WaitForVersion(storage, "booltest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "booltest", stream, 2);

            await AssertResult(nameof(WriteBoolean), storage, "booltest", 3, stream.Users.Select(x => new { x.Active }));
        }

        [Fact]
        public async Task WriteDate()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteDate), storage);

            await CreateInitialCommitWithSchema(storage, "datetest", new List<StructField>()
            {
                 new StructField("BirthDate", new DateType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO datetest
                SELECT BirthDate FROM users
            ");

            await WaitForVersion(storage, "datetest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "datetest", stream, 2);

            await AssertResult(nameof(WriteDate), storage, "datetest", 3, stream.Users.Select(x => new { x.BirthDate }));
        }

        [Fact]
        public async Task WriteDecimal()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteDecimal), storage);

            await CreateInitialCommitWithSchema(storage, "decimaltest", new List<StructField>()
            {
                 new StructField("Money", new DecimalType(19, 2), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO decimaltest
                SELECT Money / 3 as Money FROM orders
            ");

            await WaitForVersion(storage, "decimaltest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteOrder(stream.Orders[0]);

            await WaitForVersion(storage, "decimaltest", stream, 2);
            
            await AssertResult(nameof(WriteDecimal), storage, "decimaltest", 3, stream.Orders.Select(x => new { Money = Math.Round(x.Money!.Value / 3, 2) }));
        }

        [Fact]
        public async Task WriteString()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteString), storage);

            await CreateInitialCommitWithSchema(storage, "stringtest", new List<StructField>()
            {
                 new StructField("FirstName", new StringType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO stringtest
                SELECT FirstName FROM users
            ");

            await WaitForVersion(storage, "stringtest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "stringtest", stream, 2);

            await AssertResult(nameof(WriteString), storage, "stringtest", 3, stream.Users.Select(x => new { x.FirstName }));
        }

        [Fact]
        public async Task WriteTimestamp()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteTimestamp), storage);

            await CreateInitialCommitWithSchema(storage, "timestamptest", new List<StructField>()
            {
                 new StructField("BirthDate", new TimestampType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO timestamptest
                SELECT BirthDate FROM users
            ");

            await WaitForVersion(storage, "timestamptest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "timestamptest", stream, 2);

            await AssertResult(nameof(WriteTimestamp), storage, "timestamptest", 3, stream.Users.Select(x => new { x.BirthDate }));
        }

        [Fact]
        public async Task WriteList()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteList), storage);

            await CreateInitialCommitWithSchema(storage, "listtest", new List<StructField>()
            {
                 new StructField("list", new ArrayType() { ElementType = new StringType() }, true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO listtest
                SELECT list_agg(FirstName) as list FROM users GROUP BY userkey % 2
            ");

            await WaitForVersion(storage, "listtest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "listtest", stream, 2);

            var expected = stream.Users.GroupBy(x => x.UserKey % 2).Select(x => new { list = x.Select(y => y.FirstName).OrderBy(y => y).ToList() }).ToList();

            await AssertResult(nameof(WriteList), storage, "listtest", 3, expected);
        }

        [Fact]
        public async Task WriteMap()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteMap), storage);

            await CreateInitialCommitWithSchema(storage, "maptest", new List<StructField>()
            {
                 new StructField("map", new MapType(new StringType(), new StringType(), true), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO maptest
                SELECT map('firstName', FirstName) as map FROM users
            ");

            await WaitForVersion(storage, "maptest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "maptest", stream, 2);

            var expected = stream.Users.Select(x => new { map = new Dictionary<string, string>() { { "firstName", x.FirstName! } } });

            await AssertResult(nameof(WriteMap), storage, "maptest", 3, expected);
        }

        [Fact]
        public async Task WriteBinary()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(WriteBinary), storage);

            await CreateInitialCommitWithSchema(storage, "binarytest", new List<StructField>()
            {
                 new StructField("FirstName", new BinaryType(), true, new Dictionary<string, object>())
            });

            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO binarytest
                SELECT FirstName FROM users
            ");

            await WaitForVersion(storage, "binarytest", stream, 1);

            // Delete 1 user which will be 10% of users deleted which will cause a copy
            stream.DeleteUser(stream.Users[0]);

            await WaitForVersion(storage, "binarytest", stream, 2);

            await AssertResult(nameof(WriteBinary), storage, "binarytest", 3, stream.Users.Select(x => new { v = Encoding.UTF8.GetBytes(x.FirstName!) }));
        }

        [Fact]
        public async Task TestCreateTableWithStruct()
        {
            var storage = Files.Of.InternalMemory("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(TestCreateTableWithStruct), storage);

            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE subfolder.createstruct (
                    userkey INT,
                    struct STRUCT(firstName STRING, lastName STRING)
                );

                INSERT INTO subfolder.createstruct
                SELECT userKey, map('firstName', firstName, 'lastName', lastName) as struct FROM users
            ");

            await WaitForVersion(storage, "subfolder/createstruct", stream, 0);

            var firstUser = stream.Users[0];
            stream.DeleteUser(firstUser);

            await WaitForVersion(storage, "subfolder/createstruct", stream, 1);

            await AssertResult(nameof(TestCreateTableWithStruct), storage, "subfolder.createstruct", 2, stream.Users.Select(x => new { x.UserKey, @struct = new { firstName = x.FirstName, lastName = x.LastName } }));
        }

        [Fact]
        public async Task TestCreateTableWithList()
        {
            var storage = Files.Of.LocalDisk("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(TestCreateTableWithList), storage);

            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE createlist (
                    list LIST(STRING)
                );

                INSERT INTO createlist
                SELECT list_agg(firstName) as list FROM users
            ");

            await WaitForVersion(storage, "createlist", stream, 0);

            var firstUser = stream.Users[0];
            stream.DeleteUser(firstUser);

            await WaitForVersion(storage, "createlist", stream, 1);

            var expected = new[] { new { list = stream.Users.Select(x => x.FirstName).OrderBy(x => x).ToList() } };

            await AssertResult(nameof(TestCreateTableWithList), storage, "createlist", 2, expected);
        }

        /// <summary>
        /// Start a stream with the delta lake source and write to test sink and then compare the result
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="testName"></param>
        /// <param name="storage"></param>
        /// <param name="tableName"></param>
        /// <param name="waitCount"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        private async Task AssertResult<T>(string testName, IFileStorage storage, string tableName, int waitCount, IEnumerable<T> result)
        {
            DeltaLakeTestStream stream = new DeltaLakeTestStream(testName + "_compare", storage);

            await stream.StartStream($"INSERT INTO result SELECT * FROM {tableName}");

            for (int i = 0; i < waitCount; i++)
            {
                await stream.WaitForUpdate();
            }

            stream.AssertCurrentDataEqual(result);
            await stream.DisposeAsync();
        }

        private async Task CreateInitialCommitWithSchema(IFileStorage storage, string tableName, List<StructField> schemaFields)
        {
            var schemaStruct = new StructType(schemaFields);

            JsonSerializerOptions jsonOptions = new JsonSerializerOptions();
            jsonOptions.Converters.Add(new TypeConverter());

            await DeltaTransactionWriter.WriteCommit(storage, tableName, 0, new List<Internal.Delta.Actions.DeltaAction>()
            {
                new Internal.Delta.Actions.DeltaAction()
                {
                    MetaData = new Internal.Delta.Actions.DeltaMetadataAction()
                    {
                        SchemaString = JsonSerializer.Serialize(schemaStruct as SchemaBaseType, jsonOptions)
                    }
                }
            });
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
