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
using System.Buffers.Binary;
using System.Data;
using System.Text;

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    internal abstract class BaseSqlRepository : IDisposable
    {
        public Dictionary<long, HashSet<ManagedStreamPage>> ManagedPages { get; private set; }
        public List<StreamPage> UnpersistedPages { get; private set; } = [];
        private bool disposedValue;

        public StreamInfo Stream { get; }
        public SqlServerPersistentStorageSettings Settings { get; }

        protected BaseSqlRepository(StreamInfo stream, SqlServerPersistentStorageSettings settings)
        {
            Stream = stream;
            Settings = settings;
            ManagedPages = [];
        }

        public virtual void AddStreamPage(long key, byte[] value)
        {
            var page = new StreamPage(ToPageKey(key, Stream.Metadata.CurrentVersion), Stream.Metadata.StreamKey, key, value, Stream.Metadata.CurrentVersion);
            UnpersistedPages.Add(page);
            ManagedPages.AddOrCreate(new ManagedStreamPage(page.PageId, page.Version, page.PageKey));
        }


        public async Task SaveStreamPagesAsync(SqlTransaction? transaction = null)
        {
            if (UnpersistedPages.Count == 0)
            {
                return;
            }

            var pages = UnpersistedPages.ToArray();
            UnpersistedPages.Clear();
            var reader = new StreamPageDataReader(pages);

            if (transaction == null)
            {
                using var connection = new SqlConnection(Settings.ConnectionString);
                await connection.OpenAsync();
                await SaveStreamPagesAsync(reader, connection);
            }
            else
            {
                await SaveStreamPagesAsync(reader, transaction);
            }
        }

        public byte[]? Read(long key)
        {
            using var connection = new SqlConnection(Settings.ConnectionString);
            using var cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@streamKey", Stream.Metadata.StreamKey);

            string query;
            if (ManagedPages.TryGetLatestPageVersion(key, out var info))
            {
                query = "SELECT Payload FROM StreamPages WHERE PageId = @key AND StreamKey = @streamKey AND Version = @version";
                cmd.Parameters.AddWithValue("@version", info.Value.Version);
            }
            else
            {
                query = "SELECT Payload FROM StreamPages WHERE PageId = @key AND StreamKey = @streamKey";
            }

            cmd.CommandText = query;
            connection.Open();

            var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                return reader.GetFieldValue<byte[]>(0);
            }

            return null; //todo: throw?
        }

        public async Task<byte[]> ReadAsync(long key)
        {
            using var connection = new SqlConnection(Settings.ConnectionString);
            using var cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@streamKey", Stream.Metadata.StreamKey);

            string query;
            if (ManagedPages.TryGetLatestPageVersion(key, out var info))
            {
                query = "SELECT Payload FROM StreamPages WHERE PageId = @key AND StreamKey = @streamKey AND Version = @version";
                cmd.Parameters.AddWithValue("@version", info.Value.Version);
            }
            else
            {
                query = "SELECT Payload FROM StreamPages WHERE PageId = @key AND StreamKey = @streamKey";
            }

            cmd.CommandText = query;
            await connection.OpenAsync();

            var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return await reader.GetFieldValueAsync<byte[]>(0);
            }

            return []; //todo: throw?
        }

        private async Task SaveStreamPagesAsync(StreamPageDataReader reader, SqlConnection connection)
        {
            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = Settings.BulkCopySettings.DestinationTableName,
                BatchSize = Settings.BulkCopySettings.BatchSize,
                NotifyAfter = Settings.BulkCopySettings.NotifyAfter,
                EnableStreaming = Settings.BulkCopySettings.EnableStreaming,
                BulkCopyTimeout = (int)Settings.BulkCopySettings.Timeout.TotalSeconds,
                ColumnMappings =
                {
                    new SqlBulkCopyColumnMapping("PageKey", "PageKey"),
                    new SqlBulkCopyColumnMapping("StreamKey", "StreamKey"),
                    new SqlBulkCopyColumnMapping("PageId", "PageId"),
                    new SqlBulkCopyColumnMapping("Payload", "Payload"),
                    new SqlBulkCopyColumnMapping("Version", "Version"),
                },
                ColumnOrderHints = { { "PageKey", SortOrder.Ascending }, { "StreamKey", SortOrder.Ascending } }
            };

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            await bulkCopy.WriteToServerAsync(reader);
            await reader.DisposeAsync();
            await connection.DisposeAsync();
        }

        private async Task SaveStreamPagesAsync(StreamPageDataReader reader, SqlTransaction transaction)
        {
            using var bulkCopy = new SqlBulkCopy(transaction.Connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = Settings.BulkCopySettings.DestinationTableName,
                BatchSize = Settings.BulkCopySettings.BatchSize,
                NotifyAfter = Settings.BulkCopySettings.NotifyAfter,
                EnableStreaming = Settings.BulkCopySettings.EnableStreaming,
                BulkCopyTimeout = (int)Settings.BulkCopySettings.Timeout.TotalSeconds,
                ColumnMappings =
                {
                    new SqlBulkCopyColumnMapping("PageKey", "PageKey"),
                    new SqlBulkCopyColumnMapping("StreamKey", "StreamKey"),
                    new SqlBulkCopyColumnMapping("PageId", "PageId"),
                    new SqlBulkCopyColumnMapping("Payload", "Payload"),
                    new SqlBulkCopyColumnMapping("Version", "Version"),
                },
                ColumnOrderHints = { { "PageKey", SortOrder.Ascending }, { "StreamKey", SortOrder.Ascending } }
            };

            await bulkCopy.WriteToServerAsync(reader);
            await reader.DisposeAsync();
        }

        public async Task DeleteUnsuccessfulVersionsAsync()
        {
            using var connection = new SqlConnection(Settings.ConnectionString);
            using var cmd = new SqlCommand("DELETE FROM StreamPages WHERE Version > @Version AND StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@Version", Stream.Metadata.CurrentVersion);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task DeleteUnsuccessfulVersionsAsync(SqlTransaction transaction)
        {
            using var cmd = new SqlCommand("DELETE FROM StreamPages WHERE Version > @Version AND StreamKey = @StreamKey");
            cmd.Parameters.AddWithValue("@Version", Stream.Metadata.CurrentVersion);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            cmd.Transaction = transaction;
            cmd.Connection = transaction.Connection;
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task DeleteOldVersionsInDbAsync(SqlTransaction transaction)
        {
            using var cmd = new SqlCommand(@"DELETE FROM StreamPages
                WHERE StreamKey = @StreamKey AND Id NOT IN (
                    SELECT Id
                    FROM StreamPages t1
                    WHERE t1.Version = (SELECT MAX(t2.Version) FROM StreamPages t2 WHERE t2.PageId = t1.PageId)
                );", transaction.Connection);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            cmd.Transaction = transaction;

            await cmd.ExecuteNonQueryAsync();
        }

        public async Task DeleteOldVersionsInDbFromPagesAsync(IEnumerable<ManagedStreamPage> pages, SqlTransaction transaction)
        {
            if (!pages.Any())
            {
                return;
            }

            using var cmd = new SqlCommand();
            cmd.Connection = transaction.Connection;
            cmd.Transaction = transaction;
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);

            var sb = new StringBuilder("DELETE FROM StreamPages WHERE StreamKey = @streamKey AND (");
            var index = 0;
            var parameterList = new List<string>();
            foreach (var page in pages)
            {
                ManagedPages.Remove(page.PageId);
                cmd.Parameters.AddWithValue($"@p{index}", page.PageId);
                cmd.Parameters.AddWithValue($"@v{index}", page.Version);
                parameterList.Add($"(PageId = @p{index} AND Version = @v{index})");
                index++;
            }

            sb.AppendJoin(" OR ", parameterList);
            sb.Append(')');

            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync();
        }

        public static Guid ToPageKey(long pageId, int version)
        {
            Span<byte> bytes = stackalloc byte[16];
            BinaryPrimitives.WriteInt64LittleEndian(bytes, pageId);
            BinaryPrimitives.WriteInt64LittleEndian(bytes[8..], version);
            return new Guid(bytes);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                UnpersistedPages = [];
                ManagedPages = [];
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
