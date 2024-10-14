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
using System.Collections.Concurrent;
using System.Data;

namespace FlowtideDotNet.Storage.SqlServer
{
    public record StreamPage(Guid PageKey, int StreamKey, long PageId, byte[] Payload, int Version);
    public class SqlRepository : IDisposable
    {
        private readonly SqlServerPersistentStorageSettings _settings;
        private ConcurrentBag<StreamPage> Pages { get; set; }

        private readonly DataTable _table = new()
        {
            Columns =
            {
                { "PageKey", typeof(Guid) },
                { "StreamKey", typeof(int) },
                { "PageId", typeof(long) },
                { "Payload", typeof(byte[]) },
                { "Version", typeof(int) },
            }
        };

        public int CurrentVersion { get; private set; }

        public int LastSuccessfulVersion { get; private set; }

        public int CurrentPageCount => Pages.Count;

        public int? StreamKey { get; private set; }

        private readonly DebounceQueue _queue;

        public SqlRepository(SqlServerPersistentStorageSettings settings)
        {
            _settings = settings;
            Pages = [];
            _queue = new DebounceQueue(settings.ConnectionString);
        }

        public void AddStreamPage(long key, byte[] value)
        {
            if (!StreamKey.HasValue)
            {
                throw new InvalidOperationException("Stream key is not set");
            }

            Pages.Add(new StreamPage(ToPageKey(key, CurrentVersion), StreamKey.Value, key, value, CurrentVersion));

            if (Pages.Count >= _settings.WritePagesBulkLimit)
            {
                StreamPage[] pages = [.. Pages];
                Task.Run(async () => await SaveStreamPages(pages));
                Pages = [];
            }

            //_table.Rows.Add(ToPageKey(key, CurrentVersion), StreamKey, key, value, CurrentVersion);
        }

        public Task<byte[]> Read(long key)
        {
            ArgumentNullException.ThrowIfNull(StreamKey);
            _queue.Enqueue(ToPageKey(key, LastSuccessfulVersion));
            return Task.FromResult(_queue.WaitForValue(ToPageKey(key, LastSuccessfulVersion)));
            //using var connection = new SqlConnection(_settings.ConnectionString);
            //using var cmd = new SqlCommand("SELECT Payload FROM StreamPages WHERE PageId = @key AND StreamKey = @streamKey", connection);
            //await connection.OpenAsync();
            //cmd.Parameters.AddWithValue("@key", key);
            //cmd.Parameters.AddWithValue("@streamKey", StreamKey.Value);

            //var reader = await cmd.ExecuteReaderAsync();
            //if (await reader.ReadAsync())
            //{
            //    return await reader.GetFieldValueAsync<byte[]>(0);
            //}

            //return [];
        }

        public byte[] ReadSync(long key)
        {
            return Read(key).Result;
        }
        public async Task<int> PagesInDb()
        {
            using var connection = new SqlConnection(_settings.ConnectionString);
            using var cmd = new SqlCommand("SELECT COUNT(*) FROM StreamPages", connection);
            await connection.OpenAsync();
            return (int)cmd.ExecuteScalar();
        }

        public Task SaveStreamPages()
        {
            StreamPage[] pages = [.. Pages];
            Pages = [];
            return SaveStreamPages(StreamPageDataReader.Create(pages));
        }

        public Task SaveStreamPages(StreamPage[] pages)
        {
            return SaveStreamPages(StreamPageDataReader.Create(pages));

            if (pages.TryGetNonEnumeratedCount(out var count) && count == 0)
            {
                return Task.CompletedTask;
            }

            var dt = new DataTable()
            {
                Columns =
                {
                    { "PageKey", typeof(Guid) },
                    { "StreamKey", typeof(int) },
                    { "PageId", typeof(long) },
                    { "Payload", typeof(byte[]) },
                    { "Version", typeof(int) }
                }
            };

            foreach (var page in pages)
            {
                dt.Rows.Add(page.PageKey, page.StreamKey, page.PageId, page.Payload, page.Version);
            }

            return SaveStreamPages(dt);
        }

        public async Task SaveStreamPages(DataTable table)
        {
            if (table.Rows.Count == 0)
            {
                return;
            }

            using var connection = new SqlConnection(_settings.ConnectionString);
            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = _settings.BulkCopySettings.DestinationTableName,
                BatchSize = _settings.BulkCopySettings.BatchSize,
                NotifyAfter = _settings.BulkCopySettings.NotifyAfter,
                EnableStreaming = _settings.BulkCopySettings.EnableStreaming,
                BulkCopyTimeout = (int)_settings.BulkCopySettings.Timeout.TotalSeconds,
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

            //using var reader = ObjectReader.Create(pages, "PageKey", "StreamKey", "PageId", "Payload", "Version");

            //var table = _table.Copy();
            //_table.Clear();
            await connection.OpenAsync();
            await bulkCopy.WriteToServerAsync(table);
            table.Dispose();
        }

        public async Task SaveStreamPages(IDataReader reader)
        {
            using var connection = new SqlConnection(_settings.ConnectionString);
            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = _settings.BulkCopySettings.DestinationTableName,
                BatchSize = _settings.BulkCopySettings.BatchSize,
                NotifyAfter = _settings.BulkCopySettings.NotifyAfter,
                EnableStreaming = _settings.BulkCopySettings.EnableStreaming,
                BulkCopyTimeout = (int)_settings.BulkCopySettings.Timeout.TotalSeconds,
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

            //using var reader = ObjectReader.Create(pages, "PageKey", "StreamKey", "PageId", "Payload", "Version");

            //var table = _table.Copy();
            //_table.Clear();
            try
            {
                await connection.OpenAsync();
                await bulkCopy.WriteToServerAsync(reader);
                reader.Dispose();
            }
            catch (Exception ex)
            {

                throw;
            }
        }

        public async Task Delete(long key)
        {
            ArgumentNullException.ThrowIfNull(StreamKey);
            using var connection = new SqlConnection(_settings.ConnectionString);
            using var cmd = new SqlCommand("DELETE FROM StreamPages WHERE PageKey = @Key AND StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@Key", key);
            cmd.Parameters.AddWithValue("@StreamKey", StreamKey.Value);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task DeleteOldVersions()
        {
            ArgumentNullException.ThrowIfNull(StreamKey);
            using var connection = new SqlConnection(_settings.ConnectionString);
            // this one seems very slow
            //using var cmd = new SqlCommand(@"WITH CTE AS (
            //    SELECT 
            //        Id,
            //        ROW_NUMBER() OVER (PARTITION BY PageId ORDER BY Version DESC) AS RowNum
            //    FROM 
            //        StreamPages
            //    WHERE StreamKey = @StreamKey
            //)
            //DELETE FROM StreamPages
            //WHERE Id IN (
            //    SELECT Id
            //    FROM CTE
            //    WHERE RowNum > 1
            //);", connection);

            // todo: is non clustered index worth it on this select query? how big/small are the typical deletes,
            // if always/almost always like above 10% of the table we probably dont want an index?
            using var cmd = new SqlCommand(@"DELETE FROM StreamPages
                WHERE StreamKey = @StreamKey AND Id NOT IN (
                    SELECT Id
                    FROM StreamPages t1
                    WHERE t1.Version = (SELECT MAX(t2.Version) FROM StreamPages t2 WHERE t2.PageId = t1.PageId)
                );", connection);
            cmd.Parameters.AddWithValue("@StreamKey", StreamKey.Value);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task DeleteUnsuccessfulVersions()
        {
            ArgumentNullException.ThrowIfNull(StreamKey);
            using var connection = new SqlConnection(_settings.ConnectionString);
            using var cmd = new SqlCommand("DELETE FROM StreamPages WHERE Version > @Version AND StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@Version", LastSuccessfulVersion);
            cmd.Parameters.AddWithValue("@StreamKey", StreamKey.Value);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
        }

        private const string UpsertQuery = @"
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
                    END";

        public async Task<int> UpsertStream(string name)
        {
            using var connection = new SqlConnection(_settings.ConnectionString);
            using var command = new SqlCommand(UpsertQuery, connection);
            command.Parameters.Add(new SqlParameter("@UniqueStreamName", name));

            await connection.OpenAsync();
            // Execute the command and read the output
            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                StreamKey = reader.GetInt32(0);
                CurrentVersion = reader.GetInt32(1);
                LastSuccessfulVersion = CurrentVersion;
                CurrentVersion++; // are we on a new version on initialize?
                _queue.Init(StreamKey.Value);
                return StreamKey.Value;
            }

            throw new InvalidOperationException("Upsert did not return stream key for provided stream name");
        }

        public async Task UpdateStreamVersion()
        {
            ArgumentNullException.ThrowIfNull(StreamKey);
            using var connection = new SqlConnection(_settings.ConnectionString);
            using var cmd = new SqlCommand("UPDATE Streams SET LastSuccessfulVersion = @Version WHERE StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@Version", CurrentVersion);
            cmd.Parameters.AddWithValue("@StreamKey", StreamKey.Value);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
            LastSuccessfulVersion = CurrentVersion;
            CurrentVersion++;
        }

        public async Task ResetStream()
        {
            ArgumentNullException.ThrowIfNull(StreamKey);
            using var connection = new SqlConnection(_settings.ConnectionString);
            using var cmd = new SqlCommand(
                "DELETE FROM StreamPages WHERE StreamKey = @StreamKey; " +
                "UPDATE Streams SET LastSuccessfulVersion = 0 WHERE StreamKey = @StreamKey", connection);
            cmd.Parameters.AddWithValue("@StreamKey", StreamKey.Value);
            await connection.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
            LastSuccessfulVersion = 0;
            CurrentVersion = 0;
        }

        private static Guid ToPageKey(long pageId, int version)
        {
            Span<byte> bytes = stackalloc byte[16];
            BinaryPrimitives.WriteInt64LittleEndian(bytes, pageId);
            BinaryPrimitives.WriteInt64LittleEndian(bytes[8..], version);
            return new Guid(bytes);
        }

        public void Dispose()
        {

        }
    }
}
