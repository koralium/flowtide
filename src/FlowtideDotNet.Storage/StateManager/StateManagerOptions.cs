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

using FlowtideDotNet.Storage.Persistence;
using FASTER.core;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManagerOptions
    {
        public int CachePageCount { get; set; } = 10000;

        [Obsolete]
        public IDevice? LogDevice { get; set; }

        public IPersistentStorage? PersistentStorage { get; set; }

        [Obsolete]
        public string? CheckpointDir { get; set; }

        [Obsolete]
        public ICheckpointManager? CheckpointManager { get; set; }

        [Obsolete]
        public INamedDeviceFactory? TemporaryStorageFactory { get; set; }

        public FileCacheOptions? TemporaryStorageOptions { get; set; }

        [Obsolete]
        public bool RemoveOutdatedCheckpoints { get; set; } = true;

        public StateSerializeOptions SerializeOptions { get; set; } = new StateSerializeOptions();
    }
}
