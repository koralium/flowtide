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
                    name VARCHAR(100) NOT NULL,
                    strval VARCHAR(100)
                )
            ");

            // Insert some data
            await postgresFixture.ExecuteNonQueryCommand(@"
                INSERT INTO test (id, name, strval) VALUES (1, 'Test Name 1', 'test')
            ");

            PostgresTestStream stream = new PostgresTestStream(nameof(Test1), new PostgresSourceOptions()
            {
                ConnectionStringFunc = () => postgresFixture.GetConnectionString()
            });

            await stream.StartStream(@"
                CREATE TABLE test (
                    id INT,
                    name STRING
                );

                INSERT INTO output
                SELECT id, name FROM test
            ");

            await stream.WaitForUpdate();

            var result = stream.GetActualRowsAsVectors();

            await postgresFixture.ExecuteNonQueryCommand(@"
                UPDATE test SET name = 'Updated Name' WHERE id = 1
            ");

            await postgresFixture.ExecuteNonQueryCommand(@"
                INSERT INTO test (id, name) VALUES (3, 'Test Name 3')
            ");

            await stream.WaitForUpdate();
        }
    }
}