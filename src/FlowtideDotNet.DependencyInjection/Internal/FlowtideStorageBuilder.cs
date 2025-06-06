﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.DependencyInjection;

namespace FlowtideDotNet.DependencyInjection.Internal
{
    internal class FlowtideStorageBuilder : IFlowtideStorageBuilder
    {
        private readonly string name;
        private readonly IServiceCollection services;

        public FlowtideStorageBuilder(string name, IServiceCollection services)
        {
            this.name = name;
            this.services = services;
        }

        public IFlowtideStorageBuilder SetCompression(StateSerializeOptions serializeOptions)
        {
            services.AddKeyedSingleton(name, serializeOptions);
            return this;
        }

        public IFlowtideStorageBuilder SetPersistentStorage(IPersistentStorage persistentStorage)
        {
            services.AddKeyedSingleton(name, persistentStorage);
            return this;
        }

        public IFlowtideStorageBuilder SetPersistentStorage<TStorage>()
            where TStorage : class, IPersistentStorage
        {
            services.AddKeyedSingleton<IPersistentStorage, TStorage>(name);
            return this;
        }

        public IFlowtideStorageBuilder SetFileCacheFactory(IFileCacheFactory fileCacheFactory)
        {
            services.AddKeyedSingleton(name, fileCacheFactory);
            return this;
        }

        public IFlowtideStorageBuilder SetFileCacheFactory<TFileCacheFactory>()
            where TFileCacheFactory : class, IFileCacheFactory
        {
            services.AddKeyedSingleton<IFileCacheFactory, TFileCacheFactory>(name);
            return this;
        }

        public bool UseReadCache { get; set; }

        public long? MaxProcessMemory { get; set; }

        public int MinPageCount { get; set; } = 1000;

        public IServiceCollection ServiceCollection => services;

        public string Name => name;

        public int? MaxPageCount { get; set; }

        internal StateManagerOptions Build(IServiceProvider serviceProvider)
        {
            var persistentStorage = serviceProvider.GetKeyedService<IPersistentStorage>(name);
            var serializeOptions = serviceProvider.GetKeyedService<StateSerializeOptions>(name);
            var fileCacheOptions = serviceProvider.GetKeyedService<FileCacheOptions>(name);
            var fileCacheFactory = serviceProvider.GetKeyedService<IFileCacheFactory>(name);

            if (MaxProcessMemory == null && MaxPageCount == null)
            {
                var memoryInfo = GC.GetGCMemoryInfo();
                MaxProcessMemory = (long)(memoryInfo.TotalAvailableMemoryBytes * 0.8);
            }

            if (serializeOptions == null)
            {
                serializeOptions = new StateSerializeOptions();
            }

            return new StateManagerOptions()
            {
                PersistentStorage = persistentStorage,
                SerializeOptions = serializeOptions,
                UseReadCache = UseReadCache,
                TemporaryStorageOptions = fileCacheOptions,
                MaxProcessMemory = MaxProcessMemory ?? -1,
                MinCachePageCount = MinPageCount,
                FileCacheFactory = fileCacheFactory,
                CachePageCount = MaxPageCount ?? 1000
            };
        }

        public IFlowtideStorageBuilder SetPersistentStorage(Func<IServiceProvider, IPersistentStorage> func)
        {
            services.AddKeyedSingleton(name, (provider, name) =>
            {
                return func(provider);
            });

            return this;
        }
    }
}
