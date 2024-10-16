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

namespace FlowtideDotNet.Storage.SqlServer
{
    public class SqlServerPersistentSession : IPersistentStorageSession
    {
        private readonly SqlRepository repository;

        public SqlServerPersistentSession(SqlRepository repostory)
        {
            repository = repostory;
        }

        public Task Delete(long key)
        {
            return repository.Delete(key);
        }

        public void Dispose()
        {
            repository.Dispose();
        }

        public ValueTask<byte[]> Read(long key)
        {
            // probably needs some type of grouped select instead of directly by key
            return new ValueTask<byte[]>(repository.Read(key));
        }

        public Task Write(long key, byte[] value)
        {
            repository.AddStreamPage(key, value);
            return Task.CompletedTask;
        }
    }
}
