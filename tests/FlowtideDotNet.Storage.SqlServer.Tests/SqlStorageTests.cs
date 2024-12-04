using FlowtideDotNet.Storage.Persistence;
using Renci.SshNet;
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

        [Fact]
        public async Task DeletedSessionPageIsAvailableUntilCheckpoint()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 100,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(DeletedSessionPageIsAvailableUntilCheckpoint)}"));
            var pageId = 1;

            var session = storage.CreateSession();
            await session.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            await session.Commit();
            await session.Delete(pageId);
            var storedPage = await session.Read(pageId);
            await storage.CheckpointAsync([], false);
            var removedPage = await session.Read(pageId);

            Assert.NotNull(storedPage);
            Assert.Empty(removedPage);
        }

        [Fact]
        public async Task SessionPagesArePersistedOnCommit()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 100,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(SessionPagesArePersistedOnCommit)}"));
            var pageId = 1;

            var session = storage.CreateSession();
            await session.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            var unpersistedPage = await session.Read(pageId);
            await session.Commit();
            var persitedPage = await session.Read(pageId);

            Assert.Empty(unpersistedPage);
            Assert.NotEmpty(persitedPage);

            await session.Delete(pageId);
            await storage.CheckpointAsync([], false);
        }

        [Fact]
        public async Task SessionCommitWaitsForAllWrites()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            var name = $"test_{nameof(SessionCommitWaitsForAllWrites)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));
            var session = storage.CreateSession();
            var streamKey = await GetStreamKey(name);

            var numberOfPages = 5;

            for (int i = 1; i <= numberOfPages; i++)
            {
                await session.Write(i, Encoding.UTF8.GetBytes(i.ToString()));
            }

            await session.Commit();

            for (int i = 1; i <= numberOfPages; i++)
            {
                var page = await session.Read(i);
                Assert.NotEmpty(page);
                Assert.Equal(Encoding.UTF8.GetBytes(i.ToString()), page);
            }

            var numberOfPersistedPages = await _fixture.ExecuteReader<int>($"SELECT COUNT(*) FROM [dbo].[StreamPages] WHERE streamKey = {streamKey}", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });

            Assert.Equal(numberOfPages, numberOfPersistedPages);
        }

        [Fact]
        public async Task CheckpointWritesStoragePages()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            var pageId = 2;

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(CheckpointWritesStoragePages)}"));
            await storage.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            await storage.CheckpointAsync([], false);

            var hasPage = storage.TryGetValue(pageId, out var page);
            Assert.True(hasPage);
            Assert.NotNull(page);
            Assert.NotEmpty(page);
        }

        [Fact]
        public async Task StorageTryGetValueReturnsFalseForNonExistingPage()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            await storage.InitializeAsync(new StorageInitializationMetadata($"test_{nameof(StorageTryGetValueReturnsFalseForNonExistingPage)}"));

            var hasPage = storage.TryGetValue(1, out var page);
            Assert.False(hasPage);
            Assert.Null(page);
        }

        [Fact]
        public async Task StorageResetResetsVersionAndRemovesPages()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

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

        [Fact]
        public async Task StorageInitializeSetsCurrentVersion()
        {
            var settings = new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            };

            var metadata = new StorageInitializationMetadata($"test_{nameof(StorageInitializeSetsCurrentVersion)}");
            var storage = new SqlServerPersistentStorage(settings);

            await storage.InitializeAsync(metadata);
            await storage.CheckpointAsync([], false);

            storage = new SqlServerPersistentStorage(settings);
            await storage.InitializeAsync(metadata);

            Assert.Equal(2, storage.CurrentVersion);
        }

        [Fact]
        public async Task StorageInitializeRemovesUnsuccessfulPageVersions()
        {
            var settings = new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 100,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            };

            var name = $"test_{nameof(StorageInitializeRemovesUnsuccessfulPageVersions)}";
            var metadata = new StorageInitializationMetadata(name);
            var storage = new SqlServerPersistentStorage(settings);

            await storage.InitializeAsync(metadata);
            var streamKey = await GetStreamKey(name);

            var pageId = 2;
            var session = storage.CreateSession();

            await session.Write(pageId, Encoding.UTF8.GetBytes("1"));
            await session.Commit();

            await storage.CheckpointAsync([], false); // new version

            await session.Write(pageId, Encoding.UTF8.GetBytes("2"));
            await session.Commit();

            // new version was not checkpointed so it should be removed from the database
            await storage.InitializeAsync(metadata);

            var pages = await _fixture.ExecuteReader<IEnumerable<(long pageId, int version)>>(
                $"SELECT pageId, version FROM [dbo].[StreamPages] WHERE pageId = {pageId} and streamKey = {streamKey}",
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
            Assert.Equal(Encoding.UTF8.GetBytes("1"), page);
        }

        [Fact]
        public async Task StorageCompactRemovesOldVersions()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            var name = $"test_{nameof(StorageCompactRemovesOldVersions)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));
            var streamKey = await GetStreamKey(name);

            var pageId = 2;
            await storage.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            await storage.CheckpointAsync([], false);
            await storage.Write(pageId, Encoding.UTF8.GetBytes(pageId.ToString()));
            await storage.CheckpointAsync([], false);

            await storage.CompactAsync();

            var hasPage = storage.TryGetValue(pageId, out var page);

            var numberOfPages = await _fixture.ExecuteReader($"SELECT COUNT(*) FROM [dbo].[StreamPages] WHERE pageId = {pageId} AND streamKey = {streamKey}", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });

            Assert.True(hasPage);
            Assert.NotNull(page);
            Assert.NotEmpty(page);
            Assert.Equal(1, numberOfPages);
        }

        [Fact]
        public async Task SessionReadReturnsWrittenData()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            var name = $"test_{nameof(SessionReadReturnsWrittenData)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));
            var session = storage.CreateSession();

            var pageId = 2;
            var data = Guid.NewGuid().ToByteArray();

            await session.Write(pageId, data);
            await session.Commit();

            var readData = await session.Read(pageId);

            Assert.Equal(data, readData);
        }

        [Fact]
        public async Task StorageReadReturnsWrittenData()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            var name = $"test_{nameof(StorageReadReturnsWrittenData)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));

            var pageId = 2;
            var data = Guid.NewGuid().ToByteArray();

            await storage.Write(pageId, data);
            await storage.CheckpointAsync([], false);

            var hasPage = storage.TryGetValue(pageId, out var readData);

            Assert.True(hasPage);
            Assert.Equal(data, readData);
        }

        [Fact]
        public async Task InitializeWaitsForSessionBackgroundTasksAndResetsCache()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            var name = $"test_{nameof(InitializeWaitsForSessionBackgroundTasksAndResetsCache)}";
            var initMetadata = new StorageInitializationMetadata(name);
            var streamKey = await GetStreamKey(name);

            await storage.InitializeAsync(initMetadata);

            var session = storage.CreateSession();

            var pageId = 2;

            // even though a crash will occur, the pages should be persisted and then removed due to incomplete version
            await session.Write(pageId, Encoding.UTF8.GetBytes("a"));
            await session.Write(pageId + 1, Encoding.UTF8.GetBytes("b"));

            // a crash occured, init is called again
            await storage.InitializeAsync(initMetadata);

            var expectedPageBytes = Encoding.UTF8.GetBytes("c");
            await session.Write(pageId, expectedPageBytes);

            await session.Commit();
            await storage.CheckpointAsync([], false);

            var numberOfPages = await _fixture.ExecuteReader($"SELECT COUNT(*) FROM [dbo].[StreamPages] WHERE StreamKey = {streamKey}", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });

            var page = await session.Read(pageId);

            Assert.Equal(expectedPageBytes, page);
            Assert.Equal(2, numberOfPages); // checkpoint should also add one page
        }

        [Fact]
        public async Task DeletedPageIsNonReadableUntilRestore()
        {
            var storage = new SqlServerPersistentStorage(new SqlServerPersistentStorageSettings
            {
                ConnectionString = _fixture.ConnectionString,
                WritePagesBulkLimit = 1,
                BulkCopySettings = new SqlServerBulkCopySettings(),
            });

            var name = $"test_{nameof(DeletedPageIsNonReadableUntilRestore)}";
            await storage.InitializeAsync(new StorageInitializationMetadata(name));

            var session = storage.CreateSession();

            var pageId = 2;
            await session.Write(pageId, Encoding.UTF8.GetBytes("a"));
            await session.Commit();
            await session.Delete(pageId);

            var deletedPageData = await session.Read(pageId);

            await storage.RecoverAsync(1);

            var restoredPageData = await session.Read(pageId);

            Assert.Empty(deletedPageData);
            Assert.NotEmpty(restoredPageData);
        }

        private Task<int> GetStreamKey(string streamName)
        {
            return _fixture.ExecuteReader($"SELECT streamKey FROM [dbo].[Streams] WHERE UniqueStreamName = '{streamName}'", reader =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
        }
    }
}