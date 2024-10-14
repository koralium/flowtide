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
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.SqlServer
{

    // https://github.com/koralium/flowtide/issues/495
    public class SqlServerPersistentStorage : IPersistentStorage
    {
        public long CurrentVersion { get; private set; }

        private readonly SqlRepository _repo;
        public SqlServerPersistentStorage(SqlServerPersistentStorageSettings settings)
        {
            _repo = new SqlRepository(settings);
        }

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            _repo.AddStreamPage(1, metadata);
            await _repo.SaveStreamPages();
            await _repo.UpdateStreamVersion();
            CurrentVersion = _repo.CurrentVersion;
        }

        public async ValueTask CompactAsync()
        {
            await _repo.DeleteOldVersions();
        }

        public Task<int> PagesInDb()
        {
            return _repo.PagesInDb();
        }

        public IPersistentStorageSession CreateSession()
        {
            return new SqlServerPersistentSession(_repo);
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            await _repo.UpsertStream(metadata.StreamName);
            await _repo.DeleteUnsuccessfulVersions();
        }

        public ValueTask RecoverAsync(long checkpointVersion)
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask ResetAsync()
        {
            await _repo.ResetStream();
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out byte[]? value)
        {
            //throw new NotImplementedException();
            value = _repo.ReadSync(key);
            return value.Length > 0;
        }

        public ValueTask Write(long key, byte[] value)
        {
            _repo.AddStreamPage(key, value);
            // might be called from a multiple different threads, ensure write/add is safe
            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
