namespace FlowtideDotNet.Connector.Postgresql.Tests
{
    public class UnitTest1 : IClassFixture<PostgresFixture>
    {
        private readonly PostgresFixture postgresFixture;

        public UnitTest1(PostgresFixture postgresFixture)
        {
            this.postgresFixture = postgresFixture;
        }

        [Fact]
        public async Task Test1()
        {
            await postgresFixture.ExecuteNonQueryCommand(@"
                CREATE TABLE test (
                    id INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                )
            ");

            // Insert some data
            await postgresFixture.ExecuteNonQueryCommand(@"
                INSERT INTO test (id, name) VALUES (1, 'Test Name 1'), (2, 'Test Name 2')
            ");
            

        }
    }
}