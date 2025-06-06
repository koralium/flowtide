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
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using System.Buffers;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class TestStorageSession : FileCachePersistentSession
    {
        private readonly TestStorage testStorage;
        public bool HasCommitted { get; private set; } = true;

        public TestStorageSession(FileCache fileCache, TestStorage testStorage) : base(fileCache)
        {
            this.testStorage = testStorage;
        }

        public override Task Write(long key, SerializableObject value)
        {
            HasCommitted = false;
            ArrayBufferWriter<byte> bufferWriter = new ArrayBufferWriter<byte>();
            value.Serialize(bufferWriter);
            testStorage.AddWrittenKey(key, bufferWriter.WrittenSpan.ToArray());
            return base.Write(key, value);
        }

        override public Task Commit()
        {
            HasCommitted = true;
            return Task.CompletedTask;
        }
    }
}
