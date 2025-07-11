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

using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.SqlServer.Exceptions;
using System.Text;

namespace FlowtideDotNet.Storage.SqlServer.Tests
{

    public class SqlStorageTests : IClassFixture<SqlServerFixture>
    {
        private readonly SqlServerFixture _fixture;

        public SqlStorageTests(SqlServerFixture fixture)
        {
            _fixture = fixture;
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task SessionPagesArePersistedOnCommit(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(SessionPagesArePersistedOnCommit)}"));
            var pageId = 1;

            var session = storage.CreateSession();
            await session.Write(pageId, new SerializableObject(Encoding.UTF8.GetBytes(pageId.ToString())));
            await session.Commit();
            var persitedPage = await session.Read(pageId);

            Assert.NotEmpty(persitedPage.ToArray());

            await session.Delete(pageId);
            await storage.CheckpointAsync([], false);
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task ReadingNonExistingPageThrows(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(SessionPagesArePersistedOnCommit)}"));
            var session = storage.CreateSession();
            await Assert.ThrowsAsync<PageNotFoundException>(async () =>
            {
                await session.Read(1);
            });
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task SessionCommitWaitsForAllWrites(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var name = $"test_{nameof(SessionCommitWaitsForAllWrites)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));
            var session = storage.CreateSession();
            var streamKey = await GetStreamKey(name, schema);

            var numberOfPages = 5;

            for (int i = 1; i <= numberOfPages; i++)
            {
                await session.Write(i, new SerializableObject(Encoding.UTF8.GetBytes(i.ToString())));
            }

            await session.Commit();

            for (int i = 1; i <= numberOfPages; i++)
            {
                var page = await session.Read(i);
                Assert.NotEmpty(page.ToArray());
                Assert.Equal(Encoding.UTF8.GetBytes(i.ToString()), page.ToArray());
            }

            var numberOfPersistedPages = await _fixture.ExecuteReader<int>($"SELECT COUNT(*) FROM {schema}.[StreamPages] WHERE streamKey = {streamKey}", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });

            Assert.Equal(numberOfPages, numberOfPersistedPages);
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task CheckpointWritesStoragePages(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var pageId = 2;

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(CheckpointWritesStoragePages)}"));
            await storage.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            await storage.CheckpointAsync([], false);

            var hasPage = storage.TryGetValue(pageId, out var page);
            Assert.True(hasPage);
            Assert.NotNull(page);
            Assert.NotEmpty(page.Value.ToArray());
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task StorageTryGetValueReturnsFalseForNonExistingPage(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(StorageTryGetValueReturnsFalseForNonExistingPage)}"));

            var hasPage = storage.TryGetValue(1, out var page);
            Assert.False(hasPage);
            Assert.Null(page);
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task StorageResetResetsVersionAndRemovesPages(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var pageId = 2;
            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(StorageTryGetValueReturnsFalseForNonExistingPage)}"));
            var initialVersion = storage.CurrentVersion;

            await storage.Write(1, Encoding.UTF8.GetBytes(pageId.ToString()));
            await storage.CheckpointAsync([], false);
            await storage.ResetAsync();

            var hasPage = storage.TryGetValue(pageId, out var page);

            var resetVersion = storage.CurrentVersion;

            Assert.Equal(1, initialVersion);
            Assert.Equal(0, resetVersion);
            Assert.False(hasPage);
            Assert.Null(page);
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task StorageInitializeSetsCurrentVersion(string schema)
        {
            var settings = GetSettings(schema, 1);
            var metadata = new StorageInitializationMetadata($"test_{nameof(StorageInitializeSetsCurrentVersion)}");
            var storage = new SqlServerPersistentStorage(settings);

            await storage.InitializeAsync(metadata);
            await storage.CheckpointAsync([], false);

            storage = new SqlServerPersistentStorage(settings);
            await storage.InitializeAsync(metadata);

            Assert.Equal(2, storage.CurrentVersion);
        }

        [Theory]
        [InlineData("v1", true)]
        [InlineData("", true)]
        [InlineData(" ", true)]
        [InlineData(null, true)]
        [InlineData("v2", false)]
        [InlineData("", false)]
        [InlineData(" ", false)]
        [InlineData(null, false)]
        public async Task StorageInitializeIncludesVersionInStreamNameIfExists(string? version, bool useFlowtideVersioning)
        {
            var name = $"test_{nameof(StorageInitializeIncludesVersionInStreamNameIfExists)}";

            var settings = GetSettings("dbo", 1);
            settings.UseFlowtideVersioning = useFlowtideVersioning;

            StorageInitializationMetadata metadata;
            if (!string.IsNullOrWhiteSpace(version))
            {
                metadata = new StorageInitializationMetadata($"{name}", new("", version));
            }
            else
            {
                metadata = new StorageInitializationMetadata(name);
            }

            var storage = new SqlServerPersistentStorage(settings);

            await storage.InitializeAsync(metadata);

            var exists = await _fixture.ExecuteReader($"SELECT CASE WHEN EXISTS (SELECT 1 FROM Streams WHERE UniqueStreamName = '{name}-{version}') THEN 1 ELSE 0 END", reader =>
            {
                reader.Read();
                return reader.GetInt32(0) == 1;
            });

            if (settings.UseFlowtideVersioning)
            {
                Assert.True(exists != string.IsNullOrWhiteSpace(version));
            }
            else
            {
                Assert.False(exists);
            }
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task StorageInitializeRemovesUnsuccessfulPageVersions(string schema)
        {
            var settings = GetSettings(schema);

            var name = $"test_{nameof(StorageInitializeRemovesUnsuccessfulPageVersions)}";
            var metadata = new StorageInitializationMetadata(name);
            var storage = new SqlServerPersistentStorage(settings);

            await storage.InitializeAsync(metadata);
            var streamKey = await GetStreamKey(name, schema);

            var pageId = 2;
            var session = storage.CreateSession();

            await session.Write(pageId, new SerializableObject(Encoding.UTF8.GetBytes("1")));
            await session.Commit();

            await storage.CheckpointAsync([], false); // new version

            await session.Write(pageId, new SerializableObject(Encoding.UTF8.GetBytes("2")));
            await session.Commit();

            // new version was not checkpointed so it should be removed from the database
            await storage.InitializeAsync(metadata);

            var pages = await _fixture.ExecuteReader<IEnumerable<(long pageId, int version)>>(
                $"SELECT pageId, version FROM {schema}.[StreamPages] WHERE pageId = {pageId} and streamKey = {streamKey}",
                reader =>
            {
                var pages = new List<(long pageId, int version)>();
                while (reader.Read())
                {
                    pages.Add((reader.GetInt64(0), reader.GetInt32(1)));
                }

                return pages;
            });

            // create a new session and use that to read as that will not have the cached page metadata
            var newSession = storage.CreateSession();
            var page = await newSession.Read(pageId);
            Assert.All(pages, p => Assert.Equal(1, p.version));
            Assert.Equal(Encoding.UTF8.GetBytes("1"), page.ToArray());
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task StorageCompactRemovesOldVersions(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var name = $"test_{nameof(StorageCompactRemovesOldVersions)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));
            var streamKey = await GetStreamKey(name, schema);

            var pageId = 2;
            await storage.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            await storage.CheckpointAsync([], false);
            await storage.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            await storage.CheckpointAsync([], false);

            await storage.CompactAsync();

            var hasPage = storage.TryGetValue(pageId, out var page);

            var numberOfPages = await _fixture.ExecuteReader($"SELECT COUNT(*) FROM {schema}.[StreamPages] WHERE pageId = {pageId} AND streamKey = {streamKey}", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });

            Assert.True(hasPage);
            Assert.NotNull(page);
            Assert.NotEmpty(page.Value.ToArray());
            Assert.Equal(1, numberOfPages);
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task SessionReadReturnsWrittenData(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var name = $"test_{nameof(SessionReadReturnsWrittenData)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));
            var session = storage.CreateSession();

            var pageId = 2;
            var data = Guid.NewGuid().ToByteArray();

            await session.Write(pageId, new SerializableObject(data));
            await session.Commit();

            var readData = await session.Read(pageId);

            Assert.Equal(data, readData.ToArray());
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task StorageReadReturnsWrittenData(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var name = $"test_{nameof(StorageReadReturnsWrittenData)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));

            var pageId = 2;
            var data = Guid.NewGuid().ToByteArray();

            await storage.Write(pageId, data);
            await storage.CheckpointAsync([], false);

            var hasPage = storage.TryGetValue(pageId, out var readData);

            Assert.True(hasPage);
            Assert.NotNull(readData);
            Assert.Equal(data, readData.Value.ToArray());
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task InitializeWaitsForSessionBackgroundTasksAndResetsCache(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var name = $"test_{nameof(InitializeWaitsForSessionBackgroundTasksAndResetsCache)}";
            var initMetadata = new StorageInitializationMetadata(name);

            await storage.InitializeAsync(initMetadata);
            var streamKey = await GetStreamKey(name, schema);

            var session = storage.CreateSession();

            var pageId = 2;

            // even though a crash will occur, the pages should be persisted and then removed due to incomplete version
            await session.Write(pageId, new SerializableObject(Encoding.UTF8.GetBytes("a")));
            await session.Write(pageId + 1, new SerializableObject(Encoding.UTF8.GetBytes("b")));
            await session.Commit();

            // a crash occured, init is called again
            await storage.InitializeAsync(initMetadata);

            var expectedPageBytes = Encoding.UTF8.GetBytes("c");
            await session.Write(pageId, new SerializableObject(expectedPageBytes));

            await session.Commit();
            await storage.CheckpointAsync([], false);

            var numberOfPages = await _fixture.ExecuteReader($"SELECT COUNT(*) FROM {schema}.[StreamPages] WHERE StreamKey = {streamKey}", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });

            var page = await session.Read(pageId);

            Assert.Equal(expectedPageBytes, page.ToArray());
            Assert.Equal(2, numberOfPages); // checkpoint should also add one page
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task ReadingDeletedPageThrows(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var name = $"test_{nameof(ReadingDeletedPageThrows)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));

            var session = storage.CreateSession();

            var pageId = 2;
            await session.Write(pageId, new SerializableObject(Encoding.UTF8.GetBytes("a")));
            await session.Commit();
            await session.Delete(pageId);

            await Assert.ThrowsAsync<DeletedPageAccessException>(async () =>
            {
                await session.Read(pageId);
            });
        }

        [Theory]
        [InlineData("dbo")]
        [InlineData("test")]
        public async Task DeletedAndRestoredPageIsReadable(string schema)
        {
            var storage = new SqlServerPersistentStorage(GetSettings(schema));

            var name = $"test_{nameof(DeletedAndRestoredPageIsReadable)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));

            var session = storage.CreateSession();

            var pageId = 2;
            await session.Write(pageId, new SerializableObject(Encoding.UTF8.GetBytes("a")));
            await session.Commit();
            await session.Delete(pageId);
            await storage.RecoverAsync(1);

            var restoredPageData = await session.Read(pageId);

            Assert.NotEmpty(restoredPageData.ToArray());
        }

        private Task<int> GetStreamKey(string streamName, string schema)
        {
            return _fixture.ExecuteReader($"SELECT streamKey FROM {schema}.[Streams] WHERE UniqueStreamName = '{streamName}'", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
        }

        private SqlServerPersistentStorageSettings GetSettings(string schema, int writePagesBulkLimit = 100)
        {
            var settings = new SqlServerPersistentStorageSettings
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                WritePagesBulkLimit = writePagesBulkLimit,
                BulkCopySettings = new SqlServerBulkCopySettings(),
                StreamTableName = $"[{schema}].[Streams]",
                StreamPageTableName = $"[{schema}].[StreamPages]"
            };

            return settings;
        }
    }
}