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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal.DiskReader
{
    internal class LocalDiskReadManager
    {
        private readonly Dictionary<string, ILocalDiskReader> _fileReaders = new Dictionary<string, ILocalDiskReader>();
        private object _lock = new object();

        public LocalDiskReadManager()
        {
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(string fileName, long position, int length)
        {
            ILocalDiskReader? reader;
            lock (_lock)
            {
                if (!_fileReaders.TryGetValue(fileName, out reader))
                {
                    reader = new LocalDiskReaderManaged(fileName);
                    _fileReaders.Add(fileName, reader);
                }
            }
            return reader.Read(position, length);
        }

        public ValueTask<T> Read<T>(string fileName, long position, int length, IStateSerializer<T> serializer) where T : ICacheObject
        {
            ILocalDiskReader? reader;
            lock (_lock)
            {
                if (!_fileReaders.TryGetValue(fileName, out reader))
                {
                    reader = new LocalDiskReaderManaged(fileName);
                    _fileReaders.Add(fileName, reader);
                }
            }
            return reader.Read(position, length, serializer);
        }

        public void DropFile(string fileName)
        {
            lock (_lock)
            {
                if (_fileReaders.TryGetValue(fileName, out var reader))
                {
                    reader.Dispose();
                    _fileReaders.Remove(fileName);
                }
            }
        }

    }
}
