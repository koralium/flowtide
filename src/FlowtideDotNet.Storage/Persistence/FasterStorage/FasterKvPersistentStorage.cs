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

using FASTER.core;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.Persistence.FasterStorage
{
    public class FasterKvPersistentStorage : IPersistentStorage
    {
        private readonly FasterKVSettings<long, SpanByte> settings;
        private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        private Functions m_functions;
        private FasterCheckpointMetadata _checkpointMetadata;

        public FasterKvPersistentStorage(FasterKVSettings<long, SpanByte> settings)
        {
            this.settings = settings;
            m_functions = new Functions();
            m_persistentStorage = new FasterKV<long, SpanByte>(settings);
            m_adminSession = m_persistentStorage.For(m_functions).NewSession(m_functions);
            _checkpointMetadata = new FasterCheckpointMetadata(null, 0, 0, []);
        }

        public long CurrentVersion => m_persistentStorage.CurrentVersion;

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            ArrayBufferWriter<byte> bufferWriter = new ArrayBufferWriter<byte>();

            _checkpointMetadata = _checkpointMetadata.Update(metadata, m_persistentStorage.CurrentVersion);
            _checkpointMetadata.Serialize(bufferWriter);

            var memory = bufferWriter.WrittenMemory.ToArray().AsMemory();

            using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var handle = memory.Pin();
            var result = await m_adminSession.UpsertAsync(1, SpanByte.FromPinnedMemory(memory), token: tokenSource.Token);
            var status = result.Complete();
            handle.Dispose();

            var token = await TakeCheckpointAsync(includeIndex);

            
            _checkpointMetadata.PreviousCheckpoints.Add(new CheckpointInfo()
            {
                checkpointToken = token,
                checkpointVersion = m_persistentStorage.LastCheckpointedVersion
            });
        }

        internal async Task<Guid> TakeCheckpointAsync(bool includeIndex)
        {
            bool success = false;
            Guid token;
            int retryCount = 0;
            do
            {
                using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                (success, token) = await m_persistentStorage.TakeFullCheckpointAsync(CheckpointType.FoldOver, cancellationToken: tokenSource.Token).ConfigureAwait(false);
                if (!success)
                {
                    retryCount++;
                    if (retryCount > 10)
                    {
                        throw new InvalidOperationException("Failed to take checkpoint");
                    }
                }
            } while (!success);
            return token;
        }

        public IPersistentStorageSession CreateSession()
        {
            var functions = new Functions();
            var session = m_persistentStorage.For(functions).NewSession(functions);
            return new FasterKVPersistentSession(session);
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            try
            {
                await m_persistentStorage.RecoverAsync();
                await ReadCheckpointMetadata();
            }
            catch
            {

            }
        }

        private async Task ReadCheckpointMetadata()
        {
            var checkpointMetadataBytes = await Read(1);
            if (checkpointMetadataBytes.HasValue)
            {
                _checkpointMetadata = FasterCheckpointMetadata.Deserialize(new ReadOnlySequence<byte>(checkpointMetadataBytes.Value));
                m_persistentStorage.GetLatestCheckpointTokens(out var logToken, out var indexToken);
                _checkpointMetadata.PreviousCheckpoints.Add(new CheckpointInfo()
                {
                    checkpointToken = logToken,
                    checkpointVersion = _checkpointMetadata.LastCheckpointVersion
                });
            }
        }

        public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            // Remove old checkpoints
            for (int i = 0; i < _checkpointMetadata.PreviousCheckpoints.Count - 1; i++)
            {
                m_persistentStorage.CheckpointManager.Purge(_checkpointMetadata.PreviousCheckpoints[i].checkpointToken);
                _checkpointMetadata.PreviousCheckpoints.RemoveAt(i);
                i--;
            }
            if (_checkpointMetadata.LastCheckpointVersion == 1)
            {
                return ValueTask.CompletedTask;
            }
            _checkpointMetadata.ChangesSinceLastCompact += (long)changesSinceLastCompact;
            var compactionThreshold = (long)(pageCount * 0.5);
            if (_checkpointMetadata.ChangesSinceLastCompact < compactionThreshold)
            {
                return ValueTask.CompletedTask;
            }

            m_adminSession.Compact(m_persistentStorage.Log.SafeReadOnlyAddress, CompactionType.Lookup);
            _checkpointMetadata.ChangesSinceLastCompact = 0;
            return ValueTask.CompletedTask;
        }

        public ValueTask ResetAsync()
        {
            m_persistentStorage.Reset();
            return ValueTask.CompletedTask;
        }

        public async ValueTask RecoverAsync(long checkpointVersion)
        {
            var checkpointInfo = _checkpointMetadata.PreviousCheckpoints.FirstOrDefault(x => x.checkpointVersion == checkpointVersion, default);
            
            if (checkpointInfo.checkpointVersion != checkpointVersion)
            {
                throw new InvalidOperationException($"Checkpoint version {checkpointVersion} not found in metadata. Available versions: {string.Join(", ", _checkpointMetadata.PreviousCheckpoints.Select(x => x.checkpointVersion))}");
            }

            await m_persistentStorage.RecoverAsync(checkpointInfo.checkpointToken);
            await ReadCheckpointMetadata();
        }

        private async ValueTask<ReadOnlyMemory<byte>?> Read(long key)
        {
            using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var result = await m_adminSession.ReadAsync(ref key, token: tokenSource.Token);
            var (status, bytes) = result.Complete();
            return bytes;
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            if (key == 1)
            {
                value = _checkpointMetadata.Metadata;
                return _checkpointMetadata.Metadata != null;
            }
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

        public async ValueTask Write(long key, byte[] value)
        {
            using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var mem = new Memory<byte>(value);
            var handle = mem.Pin();
            var spanByte = SpanByte.FromPinnedMemory(mem);
            var result = await m_adminSession.UpsertAsync(key, spanByte, token: tokenSource.Token);
            var status = result.Complete();
            handle.Dispose();
        }

        public void Dispose()
        {
            m_adminSession.Dispose();
            m_persistentStorage.Dispose();
        }
    }
}
