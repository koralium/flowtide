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

using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence;
using Microsoft.Extensions.DependencyInjection;

namespace FlowtideDotNet.DependencyInjection
{
    public interface IFlowtideStorageBuilder
    {
        /// <summary>
        /// If read cache is enabled, persistent storage will be stored temporarily on disk.
        /// This helps to reduce the amount of requests to the persistent storage.
        /// Default: false
        /// </summary>
        bool UseReadCache { get; set; }

        /// <summary>
        /// The max amount of memory (in bytes) the process should use, default: 80% of the total memory
        /// </summary>
        long? MaxProcessMemory { get; set; }

        /// <summary>
        /// Only in use if MaxProcessMemory is not set.
        /// </summary>
        int? MaxPageCount { get; set; }

        int MinPageCount { get; set; }

        IFlowtideStorageBuilder SetPersistentStorage(IPersistentStorage persistentStorage);

        IFlowtideStorageBuilder SetPersistentStorage<TStorage>() where TStorage : class, IPersistentStorage;

        IFlowtideStorageBuilder SetPersistentStorage(Func<IServiceProvider, IPersistentStorage> func);

        IFlowtideStorageBuilder SetCompression(StateSerializeOptions serializeOptions);

        IServiceCollection ServiceCollection { get; }

        string Name { get; }
    }
}
