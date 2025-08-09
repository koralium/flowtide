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
    internal sealed class StorageRepository : BaseSqlRepository
    {
        public StorageRepository(StreamInfo stream, SqlServerPersistentStorageSettings settings)
            : base(stream, settings)
        {
        }

        public async Task ResetStream()
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            ArgumentNullException.ThrowIfNull(Stream.Metadata.StreamKey);
            using var connection = new SqlConnection(Settings.ConnectionStringFunc());
            using var cmd = new SqlCommand(
                $"DELETE FROM {Settings.StreamPageTableName} WHERE StreamKey = @StreamKey; " +
                $"UPDATE {Settings.StreamTableName} SET LastSuccessfulVersion = 0 WHERE StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task<StreamInfo> UpsertStream(string name, SqlServerPersistentStorageSettings settings)
        {
            using var connection = new SqlConnection(settings.ConnectionStringFunc());
            using var command = new SqlCommand($@"
                    DECLARE @StreamKey INT;
                    DECLARE @Version INT = 0;

                    BEGIN
                        IF NOT EXISTS (SELECT StreamKey FROM {settings.StreamTableName} WHERE UniqueStreamName = @UniqueStreamName)
                        BEGIN
                            INSERT INTO {settings.StreamTableName} (UniqueStreamName, LastSuccessfulVersion)
                            VALUES (@uniqueStreamName, @Version);
                            
                            SET @StreamKey = SCOPE_IDENTITY();
                        END
                        ELSE
                        BEGIN
                            SELECT @StreamKey = StreamKey, @Version = LastSuccessfulVersion 
                            FROM {settings.StreamTableName} 
                            WHERE UniqueStreamName = @UniqueStreamName;
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
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            using var connection = new SqlConnection(Settings.ConnectionStringFunc());
            using var cmd = new SqlCommand($"UPDATE {Settings.StreamTableName} SET LastSuccessfulVersion = @Version WHERE StreamKey = @StreamKey", connection);
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
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif

            using var cmd = new SqlCommand($"UPDATE {Settings.StreamTableName} SET LastSuccessfulVersion = @Version WHERE StreamKey = @StreamKey");
            cmd.Parameters.AddWithValue("@Version", Stream.Metadata.CurrentVersion);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            cmd.Transaction = transaction;
            cmd.Connection = transaction.Connection;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
