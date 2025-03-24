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
using FlowtideDotNet.Storage.SqlServer.Data;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.SqlServer
{
    public class SqlServerPersistentStorage : IPersistentStorage
    {
        public long CurrentVersion => _stream?.Metadata.CurrentVersion ?? 0;

        private StreamInfo? _stream;
        private StorageRepository? _storageRepository;
        private bool _disposedValue;
        private readonly SqlServerPersistentStorageSettings _settings;
        private readonly List<SessionRepository> _sessionRepositories = [];

#if DEBUG_WRITE
        private DebugWriter? _debugWriter;
#endif
        public SqlServerPersistentStorage(SqlServerPersistentStorageSettings settings)
        {
            _settings = settings;
        }

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
#if DEBUG_WRITE
            _debugWriter!.WriteCall();
#endif
            Debug.Assert(_stream != null, "Stream should be initialized");
            Debug.Assert(_storageRepository != null, "Stream repository should be initialized");

            using var connection = new SqlConnection(_settings.ConnectionStringFunc());
            await connection.OpenAsync();
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

            try
            {
                _storageRepository.AddStreamPage(1, metadata);

                await _storageRepository.SaveStreamPagesAsync(transaction);
                await _storageRepository.DeleteOldVersionsInDbAsync(transaction);

                var repoAndDeletedPages = _sessionRepositories.Select(s => new
                {
                    repo = s,
                    deletedPages = s.GetDeletedPages()
                }).ToList();

                var deletedPages = repoAndDeletedPages.SelectMany(s => s.deletedPages);
                if (deletedPages.Any())
                {
                    await _storageRepository.DeleteOldVersionsInDbFromPagesAsync(deletedPages, transaction);
                }

                // update stream version to the current version (of this checkpoint)
                await _storageRepository.UpdateStreamVersion(transaction);
                // increment the version
                _stream.IncrementVersion();

                await transaction.CommitAsync();

                // todo: this can be done before transaction commit as there's an exception/restart if commit fails?
                if (repoAndDeletedPages.Count > 0)
                {
                    foreach (var item in repoAndDeletedPages)
                    {
                        item.repo.RemoveDeletedPagesFromMemory(item.deletedPages);
                    }
                }
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        public async ValueTask CompactAsync()
        {
#if DEBUG_WRITE
            _debugWriter!.WriteCall();
#endif

            // todo: We might not actually need to do anything on compact as it's already done in checkpoint?
            Debug.Assert(_storageRepository != null, "Stream repository should be initialized");
            using var connection = new SqlConnection(_settings.ConnectionStringFunc());
            await connection.OpenAsync();
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

            try
            {
                var deletedPages = _sessionRepositories.SelectMany(s => s.GetDeletedPages());

                await _storageRepository.DeleteOldVersionsInDbAsync(transaction);
                await _storageRepository.DeleteOldVersionsInDbFromPagesAsync(deletedPages, transaction);
                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        public IPersistentStorageSession CreateSession()
        {
#if DEBUG_WRITE
            _debugWriter!.WriteCall();
#endif
            ArgumentNullException.ThrowIfNull(_stream);
            var repo = new SessionRepository(_stream, _settings);
#if DEBUG_WRITE
            repo.AddDebugWriter(_debugWriter!);
#endif
            _sessionRepositories.Add(repo);
            return new SqlServerPersistentSession(repo);
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            var name = metadata.StreamName;
            if (!string.IsNullOrWhiteSpace(metadata.StreamVersion?.Version) && _settings.UseFlowtideVersioning)
            {
                name += $"-{metadata.StreamVersion.Version}";
            }

            _stream = await StorageRepository.UpsertStream(name, _settings);
            _storageRepository = new StorageRepository(_stream, _settings);
#if DEBUG_WRITE
            _debugWriter = new DebugWriter(_stream.Metadata.Name);
            _debugWriter.WriteCall();
            _storageRepository.AddDebugWriter(_debugWriter);
#endif

            foreach (var repo in _sessionRepositories)
            {
                await repo.ClearLocalAndWaitForBackgroundTasks();
            }

            using var connection = new SqlConnection(_settings.ConnectionStringFunc());
            await connection.OpenAsync();
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

            try
            {
                await _storageRepository.DeleteUnsuccessfulVersionsAsync(transaction);
                await _storageRepository.DeleteOldVersionsInDbAsync(transaction);
                await transaction.CommitAsync();
#if DEBUG_WRITE
                _debugWriter.WriteMessage($"Initialize->transaction.CommitAsync");
#endif
            }
            catch
            {
                await transaction.RollbackAsync();
#if DEBUG_WRITE
                _debugWriter.WriteMessage($"Initialize->transaction.RollbackAsync");
#endif
                throw;
            }
        }

        public ValueTask RecoverAsync(long checkpointVersion)
        {
#if DEBUG_WRITE
            _debugWriter!.WriteCall([checkpointVersion]);
#endif

            foreach (var repository in _sessionRepositories)
            {
                repository.RestoreDeletedPages();
            }

            return ValueTask.CompletedTask;
        }

        public async ValueTask ResetAsync()
        {
            Debug.Assert(_stream != null);
            Debug.Assert(_storageRepository != null, "Stream repository should be initialized");

#if DEBUG_WRITE
            _debugWriter!.WriteCall();
#endif

            await _storageRepository.ResetStream();
            _stream.Reset();
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
#if DEBUG_WRITE
            _debugWriter!.WriteCall([key]);
#endif

            Debug.Assert(_storageRepository != null, "Stream repository should be initialized");
            var bytes = _storageRepository.Read(key);
            if (bytes != null)
            {
                value = bytes;
                return true;
            }
            value = null;
            return false;
        }

        public ValueTask Write(long key, byte[] value)
        {
#if DEBUG_WRITE
            _debugWriter!.WriteCall([key]);
#endif

            Debug.Assert(_storageRepository != null, "Stream repository should be initialized");
            _storageRepository.AddStreamPage(key, value);
            return ValueTask.CompletedTask;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    foreach (var repo in _sessionRepositories)
                    {
                        repo.Dispose();
                    }

                    _storageRepository?.Dispose();
                }
#if DEBUG_WRITE
                _debugWriter?.Dispose();
#endif
                _sessionRepositories.Clear();
                _disposedValue = true;
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
