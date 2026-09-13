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

using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// One int per page, rent counted like a tree node, shared by the state client tests.
    /// </summary>
    internal class TestPage : ICacheObject
    {
        private int _rentCount = 1;

        public TestPage(int value)
        {
            Value = value;
        }

        public int Value { get; set; }

        public bool RemovedFromCache { get; set; }

        public int RentCount => Volatile.Read(ref _rentCount);

        public bool TryRent()
        {
            var local = Volatile.Read(ref _rentCount);
            while (true)
            {
                if (local == 0)
                {
                    return false;
                }
                var observed = Interlocked.CompareExchange(ref _rentCount, local + 1, local);
                if (observed == local)
                {
                    return true;
                }
                local = observed;
            }
        }

        public void Return()
        {
            Interlocked.Decrement(ref _rentCount);
        }

        public bool TryReclaimForEviction()
        {
            return Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1;
        }

        public void EnterWriteLock() => Monitor.Enter(this);

        public void ExitWriteLock() => Monitor.Exit(this);
    }

    internal class TestPageSerializer : IStateSerializer<TestPage>
    {
        private readonly TaskCompletionSource _serializeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        private ManualResetEventSlim? _serializeGate;
        private int _gatedValue;

        /// <summary>
        /// Blocks the first serialization of a page with this value inside Serialize, the caller keeps whatever it holds.
        /// </summary>
        public void ArmSerializeGate(ManualResetEventSlim gate, int value)
        {
            _gatedValue = value;
            Volatile.Write(ref _serializeGate, gate);
        }

        public Task SerializeEntered => _serializeEntered.Task;

        private readonly TaskCompletionSource _checkpointEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// When set, CheckpointAsync waits for it, so a Commit parks inside its serializer checkpoint.
        /// </summary>
        public TaskCompletionSource? CheckpointHold { get; set; }

        public Task CheckpointEntered => _checkpointEntered.Task;

        public void Serialize(in IBufferWriter<byte> bufferWriter, in TestPage value)
        {
            if (value.Value == _gatedValue)
            {
                var gate = Interlocked.Exchange(ref _serializeGate, null);
                if (gate != null)
                {
                    _serializeEntered.TrySetResult();
                    gate.Wait();
                }
            }
            var span = bufferWriter.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(span, value.Value);
            bufferWriter.Advance(4);
        }

        public TestPage Deserialize(ReadOnlySequence<byte> bytes, int length)
        {
            var reader = new SequenceReader<byte>(bytes);
            if (!reader.TryReadLittleEndian(out int value))
            {
                throw new InvalidOperationException("Corrupt test page");
            }
            return new TestPage(value);
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
            => Serialize(bufferWriter, (TestPage)value);

        public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
            => Deserialize(bytes, length);

        public async Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata
        {
            var hold = CheckpointHold;
            if (hold != null)
            {
                _checkpointEntered.TrySetResult();
                await hold.Task;
            }
        }

        public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata => Task.CompletedTask;

        public Action? ClearTemporaryAllocationsHook { get; set; }

        public void ClearTemporaryAllocations()
        {
            ClearTemporaryAllocationsHook?.Invoke();
        }

        public volatile bool Disposed;

        public void Dispose()
        {
            Disposed = true;
        }
    }

    internal class TestMetadata : IStorageMetadata
    {
        public bool Updated { get; set; }
    }
}
