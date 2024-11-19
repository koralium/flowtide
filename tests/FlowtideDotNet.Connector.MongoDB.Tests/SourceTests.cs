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
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    public class SourceTests : IClassFixture<MongoDBFixture>
    {
        private readonly MongoDBFixture mongoDBFixture;

        public SourceTests(MongoDBFixture mongoDBFixture)
        {
            this.mongoDBFixture = mongoDBFixture;

            // Reset the database
            var mongoClient = new MongoClient(mongoDBFixture.GetConnectionString());
            var database = mongoClient.GetDatabase("test");
            var collection = database.GetCollection<User>("test");
            collection.DeleteMany(new BsonDocument());
        }

        [Fact]
        public async Task TestSource()
        {
            var stream = new MongoDBTestStream(mongoDBFixture, "test", "test", new List<string> { "id" }, "TestSource", addSource: true);

            stream.Generate(100);

            var mongoClient = new MongoClient(mongoDBFixture.GetConnectionString());
            var database = mongoClient.GetDatabase("test");
            var collection = database.GetCollection<User>("test");

            await collection.InsertManyAsync(stream.Users);

            await stream.StartStream(@"
            CREATE TABLE test.test (
                id,
                UserKey,
                FirstName,
                LastName
            );

            INSERT INTO output
            SELECT UserKey, FirstName, LastName
            FROM test.test
            ");

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

            stream.GenerateUsers(100);

            await collection.InsertManyAsync(stream.Users.Skip(100));

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

            var firstuser = stream.Users.First();
            stream.DeleteUser(firstuser);

            collection.DeleteMany(Builders<User>.Filter.Eq("UserKey", firstuser.UserKey));

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));
        }

        [Fact]
        public async Task TestStreamStopStart()
        {
            var stream = new MongoDBTestStream(mongoDBFixture, "test", "test", new List<string> { "id" }, "TestStreamStopStart", addSource: true);

            stream.Generate(100);

            var mongoClient = new MongoClient(mongoDBFixture.GetConnectionString());
            var database = mongoClient.GetDatabase("test");
            var collection = database.GetCollection<User>("test");

            await collection.InsertManyAsync(stream.Users);

            await stream.StartStream(@"
            CREATE TABLE test.test (
                id,
                UserKey,
                FirstName,
                LastName
            );

            INSERT INTO output
            SELECT UserKey, FirstName, LastName
            FROM test.test
            ");

            await stream.WaitForUpdate();

            await stream.StopStream();

            stream.Generate(100);

            await collection.InsertManyAsync(stream.Users.Skip(100));

            var firstUser = stream.Users.First();
            stream.DeleteUser(firstUser);

            collection.DeleteMany(Builders<User>.Filter.Eq("UserKey", firstUser.UserKey));

            await stream.StartStream();
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

            stream.GenerateUsers(100);

            collection.InsertMany(stream.Users.Skip(199));

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

        }

        /// <summary>
        /// Test that the stream can be stopped and started without operation time.
        /// This is a special case in comsosdb mongodb vcore which does not support operation time, only resume tokens.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestStreamStopStartWithoutOperationTime()
        {
            var stream = new MongoDBTestStream(mongoDBFixture, "test", "test", new List<string> { "id" }, "TestStreamStopStartWithoutOperationTime", addSource: true, disableOperationTime: true);

            stream.Generate(100);

            var mongoClient = new MongoClient(mongoDBFixture.GetConnectionString());
            var database = mongoClient.GetDatabase("test");
            var collection = database.GetCollection<User>("test");

            await collection.InsertManyAsync(stream.Users);

            await stream.StartStream(@"
            CREATE TABLE test.test (
                id,
                UserKey,
                FirstName,
                LastName
            );

            INSERT INTO output
            SELECT UserKey, FirstName, LastName
            FROM test.test
            ");

            await stream.WaitForUpdate();

            await stream.StopStream();

            stream.Generate(100);

            await collection.InsertManyAsync(stream.Users.Skip(100));

            var firstUser = stream.Users.First();
            stream.DeleteUser(firstUser);

            collection.DeleteMany(Builders<User>.Filter.Eq("UserKey", firstUser.UserKey));

            await stream.StartStream();
            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

            stream.GenerateUsers(100);

            collection.InsertMany(stream.Users.Skip(199));

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

        }

        [Fact]
        public async Task TestMongoDbDropCollection()
        {
            var stream = new MongoDBTestStream(mongoDBFixture, "test", "test", new List<string> { "id" }, "TestMongoDbDropCollection", addSource: true);

            stream.Generate(100);

            var mongoClient = new MongoClient(mongoDBFixture.GetConnectionString());
            var database = mongoClient.GetDatabase("test");
            var collection = database.GetCollection<User>("test");

            await collection.InsertManyAsync(stream.Users);

            await stream.StartStream(@"
            CREATE TABLE test.test (
                id,
                UserKey,
                FirstName,
                LastName
            );

            INSERT INTO output
            SELECT UserKey, FirstName, LastName
            FROM test.test
            ");

            await stream.WaitForUpdate();

            collection.Database.DropCollection("test");

            await stream.WaitForUpdate();

            while (stream.Users.Count > 0)
            {
                stream.DeleteUser(stream.Users.First());
            }

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

            stream.GenerateUsers(100);

            collection.InsertMany(stream.Users);

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));
        }

        [Fact]
        public async Task TestSelectUsingDoc()
        {
            var stream = new MongoDBTestStream(mongoDBFixture, "test", "test", new List<string> { "id" }, "TestSelectUsingDoc", addSource: true);

            stream.Generate(100);

            var mongoClient = new MongoClient(mongoDBFixture.GetConnectionString());
            var database = mongoClient.GetDatabase("test");
            var collection = database.GetCollection<User>("test");

            await collection.InsertManyAsync(stream.Users);

            await stream.StartStream(@"
            INSERT INTO output
            SELECT _doc.UserKey, _doc.FirstName, _doc.LastName
            FROM test.test
            ");

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

            stream.GenerateUsers(100);

            await collection.InsertManyAsync(stream.Users.Skip(100));

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));

            var firstuser = stream.Users.First();
            stream.DeleteUser(firstuser);

            collection.DeleteMany(Builders<User>.Filter.Eq("UserKey", firstuser.UserKey));

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(stream.Users.Select(x => new { x.UserKey, x.FirstName, x.LastName }));
        }
    }
}
