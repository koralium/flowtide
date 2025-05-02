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

using FlowtideDotNet.Storage.SqlServer.Exceptions;
using Microsoft.Data.SqlClient;
using System.Buffers.Binary;
using System.Data;
using System.Text;
using ZstdSharp.Unsafe;

namespace FlowtideDotNet.Storage.SqlServer.Data
{
    internal abstract class BaseSqlRepository : IDisposable
    {
        public Dictionary<long, ManagedStreamPage> ManagedPages { get; private set; }
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

        public DebugWriter? DebugWriter { get; private set; }
        public void AddDebugWriter(DebugWriter writer)
        {
            DebugWriter = writer;
        }

        public virtual void AddStreamPage(long key, byte[] value)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall([$"{key}", $"v{Stream.Metadata.CurrentVersion}"]);
#endif

            var page = new StreamPage(ToPageKey(key, Stream.Metadata.CurrentVersion), Stream.Metadata.StreamKey, key, value, Stream.Metadata.CurrentVersion);
            UnpersistedPages.Add(page);
            ManagedPages.AddOrReplacePage(new ManagedStreamPage(page.PageId, page.Version, page.PageKey));
        }

        public byte[]? Read(long key)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall([key]);
#endif

            using var connection = new SqlConnection(Settings.ConnectionStringFunc());
            using var cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@streamKey", Stream.Metadata.StreamKey);

            string query;
            if (ManagedPages.TryGetValue(key, out var page))
            {
#if DEBUG_WRITE
                DebugWriter.WriteMessage($"ManagedPages({key}, {page.Version})");
#endif

                if (page.ShouldDelete)
                {
                    return null;
                }

                query = $"SELECT Payload FROM {Settings.StreamPageTableName} WHERE PageId = @key AND StreamKey = @streamKey AND Version = @version";
                cmd.Parameters.AddWithValue("@version", page.Version);
            }
            else
            {
                query = $"SELECT Payload FROM {Settings.StreamPageTableName} WHERE PageId = @key AND StreamKey = @streamKey";
            }

            cmd.CommandText = query;
            connection.Open();

            var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                return reader.GetFieldValue<byte[]>(0);
            }

            return null;
        }

        public async Task<byte[]> ReadAsync(long key)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall([key]);
#endif

            using var connection = new SqlConnection(Settings.ConnectionStringFunc());
            using var cmd = new SqlCommand();
            cmd.Connection = connection;
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@streamKey", Stream.Metadata.StreamKey);

            string query;
            if (ManagedPages.TryGetValue(key, out var page))
            {
#if DEBUG_WRITE
                DebugWriter.WriteMessage($"Found ManagedPages({key}, {page.Version})");
#endif
                if (page.ShouldDelete)
                {
                    throw new DeletedPageAccessException(key, Stream.Metadata.Name);
                }

                query = $"SELECT Payload FROM {Settings.StreamPageTableName} WHERE PageId = @key AND StreamKey = @streamKey AND Version = @version";
                cmd.Parameters.AddWithValue("@version", page.Version);
            }
            else
            {
                query = $"SELECT Payload FROM {Settings.StreamPageTableName} WHERE PageId = @key AND StreamKey = @streamKey";
            }

            cmd.CommandText = query;
            await connection.OpenAsync();

            var reader = await cmd.ExecuteReaderAsync().ExecutePipeline(Settings);
            if (await reader.ReadAsync())
            {
                return await reader.GetFieldValueAsync<byte[]>(0);
            }

            throw new PageNotFoundException(key, Stream.Metadata.Name);
        }

        public async Task SaveStreamPagesAsync(SqlTransaction? transaction = null)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif
            if (UnpersistedPages.Count == 0)
            {
                return;
            }

            var pages = UnpersistedPages.ToArray();
            UnpersistedPages.Clear();
            var reader = new StreamPageDataReader(pages);

            if (transaction == null)
            {
                using var connection = new SqlConnection(Settings.ConnectionStringFunc());
                await connection.OpenAsync().ExecutePipeline(Settings);
                await SaveStreamPagesAsync(reader, connection);
            }
            else
            {
                await SaveStreamPagesAsync(reader, transaction);
            }
        }

        public async Task SaveStreamPagesAsync(StreamPageDataReader reader, SqlConnection connection)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync().ExecutePipeline(Settings);
            }

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = Settings.StreamPageTableName,
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

            await bulkCopy.WriteToServerAsync(reader).ExecutePipeline(Settings);
            await reader.DisposeAsync();
            await connection.DisposeAsync();
        }

        public async Task SaveStreamPagesAsync(StreamPageDataReader reader, SqlTransaction transaction)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif

            using var bulkCopy = new SqlBulkCopy(transaction.Connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = Settings.StreamPageTableName,
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

            await bulkCopy.WriteToServerAsync(reader).ExecutePipeline(Settings);
            await reader.DisposeAsync();
        }

        public async Task DeleteUnsuccessfulVersionsAsync()
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif

            using var connection = new SqlConnection(Settings.ConnectionStringFunc());
            using var cmd = new SqlCommand($"DELETE FROM {Settings.StreamPageTableName} WHERE Version > @Version AND StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@Version", Stream.Metadata.CurrentVersion);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            await connection.OpenAsync().ExecutePipeline(Settings);
            await cmd.ExecuteNonQueryAsync().ExecutePipeline(Settings);
        }

        public async Task DeleteUnsuccessfulVersionsAsync(SqlTransaction transaction)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif

            using var cmd = new SqlCommand($"DELETE FROM {Settings.StreamPageTableName} WHERE Version > @Version AND StreamKey = @StreamKey");
            cmd.Parameters.AddWithValue("@Version", Stream.Metadata.LastSucessfulVersion);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            cmd.Transaction = transaction;
            cmd.Connection = transaction.Connection;
            await cmd.ExecuteNonQueryAsync().ExecutePipeline(Settings);
        }

        public async Task DeleteOldVersionsInDbAsync(SqlTransaction transaction)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif

            using var cmd = new SqlCommand($@"DELETE FROM {Settings.StreamPageTableName}
                WHERE StreamKey = @StreamKey AND Id IN (
                SELECT Id
                FROM {Settings.StreamPageTableName} t1
                WHERE t1.Version < (SELECT MAX(t2.Version) 
                	FROM {Settings.StreamPageTableName} t2 
                	WHERE t2.PageId = t1.PageId AND StreamKey = @StreamKey)
                );", transaction.Connection);
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);
            cmd.Transaction = transaction;

            await cmd.ExecuteNonQueryAsync().ExecutePipeline(Settings);
        }

        public async Task DeleteOldVersionsInDbFromPagesAsync(IEnumerable<ManagedStreamPage> pages, SqlTransaction transaction)
        {
#if DEBUG_WRITE
            DebugWriter!.WriteCall();
#endif

            if (!pages.Any())
            {
                return;
            }

            using var cmd = new SqlCommand();
            cmd.Connection = transaction.Connection;
            cmd.Transaction = transaction;
            cmd.Parameters.AddWithValue("@StreamKey", Stream.Metadata.StreamKey);

            var sb = new StringBuilder($"DELETE FROM {Settings.StreamPageTableName} WHERE StreamKey = @streamKey AND (");
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
            await cmd.ExecuteNonQueryAsync().ExecutePipeline(Settings);
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

        public void ClearLocal()
        {
            UnpersistedPages = [];
            ManagedPages = [];
        }
    }
}
