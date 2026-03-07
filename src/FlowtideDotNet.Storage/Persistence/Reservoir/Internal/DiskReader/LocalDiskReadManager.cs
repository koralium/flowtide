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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader
{
    internal class LocalDiskReadManager
    {
        private readonly Dictionary<string, ILocalDiskFile> _fileReaders = new Dictionary<string, ILocalDiskFile>();
        private readonly IMemoryAllocator memoryAllocator;
        private object _lock = new object();

        public LocalDiskReadManager(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }

        private ILocalDiskFile GetOrCreateReader(string fileName)
        {
            lock (_lock)
            {
                if (!_fileReaders.TryGetValue(fileName, out var reader))
                {
                    if (Environment.OSVersion.Platform == PlatformID.Unix)
                    {
                        reader = new LocalDiskReaderUnix(fileName, 4096, memoryAllocator);
                        _fileReaders.Add(fileName, reader);
                        return reader;
                    }
                    else
                    {
                        reader = new LocalDiskReaderManaged(fileName);
                        _fileReaders.Add(fileName, reader);
                    }
                    return reader;
                }
                return reader;
            }
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(string fileName, long position, int length, uint crc32)
        {
            ILocalDiskFile reader = GetOrCreateReader(fileName);
            return reader.Read(position, length, crc32);
        }

        public ValueTask<T> Read<T>(string fileName, long position, int length, uint crc32, IStateSerializer<T> serializer) where T : ICacheObject
        {
            ILocalDiskFile reader = GetOrCreateReader(fileName);
            return reader.Read(position, length, crc32, serializer);
        }

        public Task Write(string fileName, PipeReader reader)
        {
            ILocalDiskFile file = GetOrCreateReader(fileName);
            return file.Write(reader);
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
