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

using Microsoft.Data.SqlClient;
using System.Data;

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    internal sealed class StreamRepository : BaseSqlRepository
    {
        public StreamRepository(StreamInfo stream, SqlServerPersistentStorageSettings settings)
            : base(stream, settings)
        {
        }

        public async Task ResetStream()
        {
            ArgumentNullException.ThrowIfNull(Stream.Metadata.StreamKey);
            using var connection = new SqlConnection(Settings.ConnectionString);
            using var cmd = new SqlCommand(
                "DELETE FROM StreamPages WHERE StreamKey = @StreamKey; " +
                "UPDATE Streams SET LastSuccessfulVersion = 0 WHERE StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
            Stream.Reset();
        }

        public static async Task<StreamInfo> UpsertStream(string name, string connectionString)
        {
            using var connection = new SqlConnection(connectionString);
            using var command = new SqlCommand(@"
                    DECLARE @StreamKey INT;
                    DECLARE @Version INT = 0;

                    BEGIN
                        IF NOT EXISTS (SELECT StreamKey FROM Streams WHERE UniqueStreamName = @UniqueStreamName)
                        BEGIN
                            INSERT INTO Streams (UniqueStreamName, LastSuccessfulVersion)
                            VALUES (@uniqueStreamName, @Version);
                            
                            SET @StreamKey = SCOPE_IDENTITY();
                        END
                        ELSE
                        BEGIN
                            SELECT @StreamKey = StreamKey, @Version = LastSuccessfulVersion FROM Streams WHERE UniqueStreamName = @UniqueStreamName;
                        END

                        SELECT @StreamKey AS StreamKey, @Version AS Version;
                    END", connection);
            command.Parameters.Add(new SqlParameter("@UniqueStreamName", name));

            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var lastSuccessfulVersion = reader.GetInt32(1);
                var currentVersion = lastSuccessfulVersion + 1;
                return new StreamInfo(name, reader.GetInt32(0), currentVersion, lastSuccessfulVersion);
            }

            throw new InvalidOperationException($"Upsert did not return stream key for provided stream name \"{name}\"");
        }

        public async Task UpdateStreamVersion()
        {
            using var connection = new SqlConnection(Settings.ConnectionString);
            using var cmd = new SqlCommand("UPDATE Streams SET LastSuccessfulVersion = @Version WHERE StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@Version", Stream.Metadata.CurrentVersion);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            await cmd.ExecuteNonQueryAsync();
        }

        public async Task UpdateStreamVersion(SqlTransaction transaction)
        {
            using var cmd = new SqlCommand("UPDATE Streams SET LastSuccessfulVersion = @Version WHERE StreamKey = @StreamKey");
            cmd.Parameters.AddWithValue("@Version", Stream.Metadata.CurrentVersion);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            cmd.Transaction = transaction;
            cmd.Connection = transaction.Connection;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
