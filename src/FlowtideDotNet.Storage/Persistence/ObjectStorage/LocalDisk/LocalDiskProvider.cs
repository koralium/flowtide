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

using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalDisk
{
    public class LocalDiskProvider : IFileStorageProvider
    {
        private readonly string basePath;

        public LocalDiskProvider(string basePath)
        {
            this.basePath = basePath;
        }
        public Task<IEnumerable<string>> ListFilesAsync(string path)
        {
            path = Path.Combine(basePath, path);
            if (Directory.Exists(path))
            {
                return Task.FromResult(Directory.EnumerateFiles(path));
            }

            return Task.FromResult(Enumerable.Empty<string>());
        }

        public PipeReader OpenReadFile(string path)
        {
            if (!path.StartsWith(basePath))
            {
                path = Path.Combine(basePath, path);
            }
            
            return PipeReader.Create(File.OpenRead(path));
        }

        public async ValueTask<T> Read<T>(string path, int offset, int length, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            path = Path.Combine(basePath, path);
            using var stream = File.OpenRead(path);
            stream.Seek(offset, SeekOrigin.Begin);

            var buffer = new byte[length];
            await stream.ReadExactlyAsync(buffer, 0, length);

            return stateSerializer.Deserialize(new ReadOnlySequence<byte>(buffer), length);
        }

        public ReadOnlyMemory<byte> GetMemory(string path, int offset, int length)
        {
            if (!path.StartsWith(basePath))
            {
                path = Path.Combine(basePath, path);
            }

            var stream = System.IO.File.OpenRead(path);
            stream.Position = offset;
            var buffer = new byte[length];
            stream.ReadExactly(buffer, 0, length);
            return buffer;
        }

        public async ValueTask WriteFile(PipeReader data, string path)
        {
            path = Path.Combine(basePath, path);
            var directory = Path.GetDirectoryName(path);
            if (directory != null && !Directory.Exists(directory)) 
            {
                Directory.CreateDirectory(directory);
            }
            
            var filewrite = File.OpenWrite(path);
            await data.CopyToAsync(filewrite);
            await filewrite.FlushAsync();
            await filewrite.DisposeAsync();
        }
    }
}
