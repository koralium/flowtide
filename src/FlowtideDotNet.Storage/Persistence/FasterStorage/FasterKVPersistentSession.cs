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

        public void Delete(long key)
        {
            var status = session.Delete(key);
            if (!status.IsCompleted || status.IsPending)
            {
                session.CompletePending(true);
            }
        }

        public byte[] Read(long key)
        {
            var result = session.Read(key);
            if (result.status.IsCompleted && !result.status.IsPending)
            {
                return result.output;
            }
            if (session.CompletePendingWithOutputs(out var completedOutputs, true))
            {
                var hasNext = completedOutputs.Next();
                var bytes = completedOutputs.Current.Output;
                hasNext = completedOutputs.Next();
                if (hasNext)
                {
                    throw new Exception();
                }
                return bytes;
            }
            else
            {
                throw new Exception();
            }
        }

        public void Write(long key, byte[] value)
        {
            var mem = new Memory<byte>(value);
            var handle = mem.Pin();
            var spanByte = SpanByte.FromPinnedMemory(value);
            var status = session.Upsert(key, spanByte);
            if (!status.IsCompleted || status.IsPending)
            {
                session.CompletePending(true);
            }
            handle.Dispose();
        }
    }
}
