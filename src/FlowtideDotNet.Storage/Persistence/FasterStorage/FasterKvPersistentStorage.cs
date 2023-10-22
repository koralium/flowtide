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
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.Persistence.FasterStorage
{
    public class FasterKvPersistentStorage : IPersistentStorage
    {
        private readonly FasterKVSettings<long, SpanByte> settings;
        private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        private Functions m_functions;

        public FasterKvPersistentStorage(FasterKVSettings<long, SpanByte> settings)
        {
            this.settings = settings;
            m_functions = new Functions();
            m_persistentStorage = new FasterKV<long, SpanByte>(settings);
            m_adminSession = m_persistentStorage.For(m_functions).NewSession(m_functions);
        }

        public long CurrentVersion => m_persistentStorage.CurrentVersion;

        public async ValueTask CheckpointAsync(byte[] metadata)
        {
            Memory<byte> memory = metadata.AsMemory();
            var handle = memory.Pin();
            var status = m_adminSession.Upsert(1, SpanByte.FromPinnedMemory(memory));
            if (status.IsCompleted || status.IsPending)
            {
                m_adminSession.CompletePending(true);
            }
            handle.Dispose();
            
            await TakeCheckpointAsync();
        }

        internal async Task<Guid> TakeCheckpointAsync()
        {
            bool success = false;
            Guid token;
            do
            {
                (success, token) = await m_persistentStorage.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
            } while (!success);
            return token;
        }

        public IPersistentStorageSession CreateSession()
        {
            var functions = new Functions();
            var session = m_persistentStorage.For(functions).NewSession(functions);
            return new FasterKVPersistentSession(session);
        }

        public async Task InitializeAsync()
        {
            try
            {
                await m_persistentStorage.RecoverAsync();
            }
            catch
            {

            }
        }

        public async ValueTask CompactAsync()
        {
            m_adminSession.Compact(m_persistentStorage.Log.SafeReadOnlyAddress, CompactionType.Lookup);
            m_persistentStorage.Log.Truncate();
            await m_persistentStorage.TakeFullCheckpointAsync(CheckpointType.Snapshot);
        }

        public ValueTask ResetAsync()
        {
            m_persistentStorage.Reset();
            return ValueTask.CompletedTask;
        }

        public async ValueTask RecoverAsync(long checkpointVersion)
        {
            await m_persistentStorage.RecoverAsync(recoverTo: checkpointVersion);
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out byte[]? value)
        {
            var result = m_adminSession.Read(key);
            if (result.status.Found || result.status.IsPending)
            {
                if (result.status.IsCompleted && !result.status.IsPending)
                {
                    value = result.output;
                    return true;
                }

                if (m_adminSession.CompletePendingWithOutputs(out var completedOutputs, true))
                {
                    var hasNext = completedOutputs.Next();
                    if (!hasNext)
                    {
                        value = null;
                        return false;
                    }
                    var bytes = completedOutputs.Current.Output;
                    hasNext = completedOutputs.Next();
                    if (hasNext)
                    {
                        throw new Exception();
                    }
                    value = bytes;
                    return true;
                }
            }
            value = default;
            return false;
        }

        public void Write(long key, byte[] value)
        {
            var mem = new Memory<byte>(value);
            var handle = mem.Pin();
            var spanByte = SpanByte.FromPinnedMemory(mem);
            var status = m_adminSession.Upsert(key, spanByte);
            if (!status.IsCompleted || status.IsPending)
            {
                m_adminSession.CompletePending(true);
            }
            handle.Dispose();
        }

        public void Dispose()
        {
            m_adminSession.Dispose();
            m_persistentStorage.Dispose();
        }
    }
}
