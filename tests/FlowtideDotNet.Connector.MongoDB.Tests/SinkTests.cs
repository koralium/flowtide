using MongoDB.Bson;
using MongoDB.Driver;
using System.IO;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    public class SinkTests : IClassFixture<MongoDBFixture>
    {
        private readonly MongoDBFixture mongoDBFixture;

        public SinkTests(MongoDBFixture mongoDBFixture)
        {
            this.mongoDBFixture = mongoDBFixture;
        }

        [Fact]
        public async Task TestInsert()
        {
            var userCount = 1000;
            MongoDBTestStream testStream = new MongoDBTestStream(
                mongoDBFixture,
                "test",
                "test",
                new List<string>() { "UserKey" }, "test");

            testStream.Generate(userCount);
            await testStream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey,
                FirstName,
                LastName
            FROM users
            ");

            var mongoClient = new MongoClient(mongoDBFixture.GetConnectionString());
            var database = mongoClient.GetDatabase("test");
            var collection = database.GetCollection<BsonDocument>("test");
            collection.Indexes.CreateOne(new CreateIndexModel<BsonDocument>(Builders<BsonDocument>.IndexKeys.Ascending("UserKey")));

            
            while (true)
            {
                var count = await collection.CountDocumentsAsync(new BsonDocument());
                if (count == userCount)
                {
                    break;
                }
                await Task.Delay(100);
            }

            var user = testStream.Users.First();
            user.FirstName = "updated";
            testStream.AddOrUpdateUser(user);

            while (true)
            {
                var doc = collection.Find(Builders<BsonDocument>.Filter.Eq("UserKey", new BsonInt64(user.UserKey))).FirstOrDefault();
                if (doc?.GetElement("FirstName").Value.AsString == "updated")
                {
                    break;
                }
                await testStream.SchedulerTick();
                await Task.Delay(100);
            }
        }
    }
}