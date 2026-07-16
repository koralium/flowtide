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
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    /// <summary>
    /// Keeps the stored files alive when the storage is disposed, so state survives a restart.
    /// </summary>
    internal sealed class KeepAliveMemoryFileProvider : MemoryFileProvider, Storage.Persistence.Reservoir.IReservoirStorageProvider
    {
        public new void Dispose()
        {
        }
    }

    /// <summary>
    /// Test stream that uses a shared file provider, so two streams can share persisted state.
    /// </summary>
    internal sealed class SharedStorageTestStream : FlowtideTestStream
    {
        private readonly KeepAliveMemoryFileProvider _fileProvider;

        public SharedStorageTestStream(string testName, KeepAliveMemoryFileProvider fileProvider) : base(testName)
        {
            _fileProvider = fileProvider;
        }

        protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            return new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = _fileProvider });
        }
    }
}
