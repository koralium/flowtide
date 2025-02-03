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

using FlowtideDotNet.Storage.StateManager.Internal;

namespace FlowtideDotNet.Storage.Persistence.CacheStorage
{
    internal class FileCachePersistentSession : IPersistentStorageSession
    {
        private readonly FlowtideDotNet.Storage.FileCache.FileCache fileCache;

        public FileCachePersistentSession(FlowtideDotNet.Storage.FileCache.FileCache fileCache)
        {
            this.fileCache = fileCache;
        }

        public virtual Task Commit()
        {
            return Task.CompletedTask;
        }

        public virtual Task Delete(long key)
        {
            fileCache.Free(key);
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(long key)
        {
            return ValueTask.FromResult(fileCache.Read(key));
        }

        public ValueTask<T> Read<T>(long key, IStateSerializer<T> serializer)
            where T : ICacheObject
        {
            return ValueTask.FromResult(fileCache.Read(key, serializer));
        }

        public virtual Task Write(long key, SerializableObject value)
        {
            fileCache.Write(key, value);
            fileCache.Flush();
            return Task.CompletedTask;
        }
    }
}
