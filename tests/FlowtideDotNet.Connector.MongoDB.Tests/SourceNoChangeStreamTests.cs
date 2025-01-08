using FlowtideDotNet.AcceptanceTests.Entities;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    [Collection("MongoDB Tests")]
    public class SourceNoChangeStreamTests : IClassFixture<MongoDbNoChangeStreamFixture>
    {
        private readonly MongoDbNoChangeStreamFixture mongoDBFixture;

        public SourceNoChangeStreamTests(MongoDbNoChangeStreamFixture mongoDbNoChangeStreamFixture)
        {
            mongoDBFixture = mongoDbNoChangeStreamFixture;
        }

        [Fact]
        public async Task TestSourceWithNoChangeStream()
        {
            var stream = new MongoDBTestStream(mongoDBFixture, "test", "test", new List<string> { "id" }, "TestSourceWithNoChangeStream", addSource: true);

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
    }
}
