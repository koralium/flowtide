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
        private StreamRepository? _streamRepository;
        private bool _disposedValue;
        private readonly SqlServerPersistentStorageSettings _settings;
        private readonly List<StreamPageRepository> _sessionRepositories = [];
        public SqlServerPersistentStorage(SqlServerPersistentStorageSettings settings)
        {
            _settings = settings;
        }

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            Debug.Assert(_stream != null, "Stream should be initialized");
            Debug.Assert(_streamRepository != null, "Stream repository should be initialized");

            using var connection = new SqlConnection(_settings.ConnectionString);
            await connection.OpenAsync();
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

            try
            {
                _streamRepository.AddStreamPage(1, metadata);

                await _streamRepository.SaveStreamPagesAsync(transaction);
                await _streamRepository.DeleteOldVersionsInDbAsync(transaction);
                await _streamRepository.DeleteUnsuccessfulVersionsAsync(transaction);

                var deletedPages = _sessionRepositories.SelectMany(s => s.GetDeletedPages()).ToList();
                if (deletedPages.Count > 0)
                {
                    await _streamRepository.DeleteOldVersionsInDbFromPagesAsync(deletedPages, transaction);
                }

                // update stream version to the current version (of this checkpoint)
                await _streamRepository.UpdateStreamVersion(transaction);
                // increment the version
                _stream.IncrementVersion();

                await transaction.CommitAsync();

                // todo: this can be done before transaction commit as there's an exception/restart if commit fails?
                if (deletedPages.Count > 0)
                {
                    foreach (var repo in _sessionRepositories)
                    {
                        repo.RemoveDeletedPagesFromMemory(deletedPages);
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
            // todo: We might not actually need to do anything on compact as it's already done in checkpoint?
            Debug.Assert(_streamRepository != null, "Stream repository should be initialized");
            using var connection = new SqlConnection(_settings.ConnectionString);
            await connection.OpenAsync();
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

            try
            {
                var deletedPages = _sessionRepositories.SelectMany(s => s.GetDeletedPages());

                await _streamRepository.DeleteOldVersionsInDbAsync(transaction);
                await _streamRepository.DeleteOldVersionsInDbFromPagesAsync(deletedPages, transaction);
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
            ArgumentNullException.ThrowIfNull(_stream);
            var repo = new StreamPageRepository(_stream, _settings);
            _sessionRepositories.Add(repo);
            return new SqlServerPersistentSession(repo);
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            _stream = await StreamRepository.UpsertStream(metadata.StreamName, _settings.ConnectionString);
            _streamRepository = new StreamRepository(_stream, _settings);
            using var connection = new SqlConnection(_settings.ConnectionString);
            await connection.OpenAsync();
            using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

            try
            {
                await _streamRepository.DeleteOldVersionsInDbAsync(transaction);
                await _streamRepository.DeleteUnsuccessfulVersionsAsync(transaction);
                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        public ValueTask RecoverAsync(long checkpointVersion)
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask ResetAsync()
        {
            Debug.Assert(_stream != null);
            Debug.Assert(_streamRepository != null, "Stream repository should be initialized");
            await _streamRepository.ResetStream();
            _stream.Reset();
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out byte[]? value)
        {
            Debug.Assert(_streamRepository != null, "Stream repository should be initialized");
            value = _streamRepository.Read(key);
            return value != null;
        }

        public ValueTask Write(long key, byte[] value)
        {
            Debug.Assert(_streamRepository != null, "Stream repository should be initialized");
            _streamRepository.AddStreamPage(key, value);
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

                    _streamRepository?.Dispose();
                }

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
