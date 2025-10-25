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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    internal class BlobPersistentSession : IPersistentStorageSession
    {
        private const int MaxFileSize = 10 * 1024 * 1024;
        private BlobFileWriter _fileWriter;
        private Memory<byte> _fileHeaderBytes;

        public BlobPersistentSession()
        {
            SetupFileWriter();
        }

        [MemberNotNull(nameof(_fileWriter))]
        private void SetupFileWriter()
        {
            _fileWriter = new BlobFileWriter(MemoryPool<byte>.Shared);

            // Take out the bytes for the header
            _fileHeaderBytes = _fileWriter.GetMemory(64);
            _fileWriter.Advance(64);
        }

        public Task Commit()
        {
            throw new NotImplementedException();
        }

        public Task Delete(long key)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public ValueTask<T> Read<T>(long key, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            throw new NotImplementedException();
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(long key)
        {
            throw new NotImplementedException();
        }

        public Task Write(long key, SerializableObject value)
        {
            var position = _fileWriter.WrittenLength;
            value.Serialize(_fileWriter);

            if (_fileWriter.WrittenLength >= MaxFileSize)
            {
                // TODO: Send the file to the blob writer

                SetupFileWriter();
            }


            return Task.CompletedTask;
        }
    }
}
