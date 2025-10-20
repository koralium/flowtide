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

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    internal class ObjectPersistentStorage : IPersistentStorage
    {
        public ObjectPersistentStorage()
        {
            
        }

        public long CurrentVersion => throw new NotImplementedException();

        public ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            throw new NotImplementedException();
        }

        public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            throw new NotImplementedException();
        }

        public IPersistentStorageSession CreateSession()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            throw new NotImplementedException();
        }

        public ValueTask RecoverAsync(long checkpointVersion)
        {
            throw new NotImplementedException();
        }

        public ValueTask ResetAsync()
        {
            throw new NotImplementedException();
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            throw new NotImplementedException();
        }

        public ValueTask Write(long key, byte[] value)
        {
            throw new NotImplementedException();
        }
    }
}
