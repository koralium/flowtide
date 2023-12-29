namespace FlowtideDotNet.Connector.CosmosDB.Tests
{
    public class UnitTest1
    {
        [Fact(Skip = "Requires local cosmosdb emulator")]
        public async Task TesWithPartitionKey()
        {
            CosmosDbTestStream testStream = new CosmosDbTestStream("test");
            testStream.Generate();

            await testStream.StartStream(@"
               INSERT INTO output 
                SELECT 
                    UserKey as id,
                    FirstName,
                    LastName,
                    UserKey as pk
                FROM users");

            await testStream.WaitForUpdate();
        }

        [Fact(Skip = "Requires local cosmosdb emulator")]
        public async Task TesWithoutPartitionKey()
        {
            CosmosDbTestStream testStream = new CosmosDbTestStream("testnopk");
            testStream.Generate();

            await testStream.StartStream(@"
               INSERT INTO output 
                SELECT 
                    UserKey as id,
                    FirstName,
                    LastName,
                    '1' as pk
                FROM users");

            await testStream.WaitForUpdate();
        }
    }
}