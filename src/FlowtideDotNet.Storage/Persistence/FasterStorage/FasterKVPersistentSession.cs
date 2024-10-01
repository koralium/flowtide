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

using FlowtideDotNet.Storage.StateManager.Internal;
using FASTER.core;

namespace FlowtideDotNet.Storage.Persistence.FasterStorage
{
    internal class FasterKVPersistentSession : IPersistentStorageSession
    {
        private readonly ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session;

        public FasterKVPersistentSession(ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> session)
        {
            this.session = session;
        }

        public async Task Delete(long key)
        {
            using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var result = await session.DeleteAsync(key, token: tokenSource.Token);
            _ = result.Complete();
        }

        public void Dispose()
        {
            session.Dispose();
        }

        public async ValueTask<byte[]> Read(long key)
        {
            using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var input = new SpanByte();
            var result = await session.ReadAsync(ref key, token: tokenSource.Token);
            var (status, bytes) = result.Complete();
            if (bytes == null)
            {
                throw new InvalidOperationException("Could not read from persistent storage");
            }
            return bytes;
        }

        public async Task Write(long key, byte[] value)
        {
            var mem = new Memory<byte>(value);
            var handle = mem.Pin();
            var spanByte = SpanByte.FromPinnedMemory(value);
            using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                var result = await session.UpsertAsync(key, spanByte, token: tokenSource.Token);
                var status = result.Complete();
            }
            catch(Exception e)
            {
                throw;
            }
            
            handle.Dispose();
        }
    }
}
