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
using System.Threading.Channels;

namespace FlowtideDotNet.Storage.StateManager.WorkerThread
{
    internal class StateWorker
    {
        private readonly Channel<WorkerMessage> m_channel;

        private readonly FasterLog m_temporaryStorage;
        private readonly FasterKV<long, SpanByte> m_persistentStorage;
        private Dictionary<long, long> m_modified = new Dictionary<long, long>();
        private Functions m_functions;
        private ClientSession<long, SpanByte, SpanByte, byte[], long, Functions> m_adminSession;
        private Task _ioTask;

        public StateWorker(StateManagerOptions options)
        {
            m_channel = Channel.CreateUnbounded<WorkerMessage>();

            m_temporaryStorage = new FASTER.core.FasterLog(new FASTER.core.FasterLogSettings()
            {
                AutoCommit = true,
                LogDevice = options.TemporaryStorageFactory.Get(new FileDescriptor()
                {
                    directoryName = "tmp",
                    fileName = "log"
                })
            });
            m_persistentStorage = new FasterKV<long, SpanByte>(new FasterKVSettings<long, SpanByte>()
            {
                RemoveOutdatedCheckpoints = true,
                MemorySize = 1024 * 1024 * 16,
                PageSize = 1024 * 1024,
                LogDevice = options.LogDevice,
                CheckpointManager = options.CheckpointManager,
                CheckpointDir = options.CheckpointDir,
                //ConcurrencyControlMode = ConcurrencyControlMode.RecordIsolation
            });

            m_functions = new Functions();
            m_adminSession = m_persistentStorage.For(m_functions).NewSession(m_functions);

            _ioTask = Task.Factory.StartNew(Loop, TaskCreationOptions.LongRunning)
                .Unwrap();
        }

        private async Task Loop()
        {
            while (!m_channel.Reader.Completion.IsCompleted)
            {
                var msg = await m_channel.Reader.ReadAsync();
                if (msg is ReadMessage readMessage)
                {
                    await HandleReadMessage(readMessage);
                }
            }
        }

        /// <summary>
        /// Handles reading from persistent storage
        /// </summary>
        /// <param name="readMessage"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task HandleReadMessage(ReadMessage readMessage)
        {
            if (m_modified.TryGetValue(readMessage.PageId, out var location))
            {
                var (bytes, length) = await m_temporaryStorage.ReadAsync(location);
                readMessage.TaskCompletionSource.SetResult(bytes);
            }
            var op = await m_adminSession.ReadAsync(readMessage.PageId);
            var (status, persistentBytes) = op.Complete();

            if (!status.Found || !status.IsCompletedSuccessfully)
            {
                throw new Exception();
            }
            readMessage.TaskCompletionSource.SetResult(persistentBytes);
        }

        public Task<byte[]> ReadAsync(long pageId)
        {
            var completionSource = new TaskCompletionSource<byte[]>();
            if (!m_channel.Writer.TryWrite(new ReadMessage(pageId, completionSource)))
            {
                throw new Exception();
            }
            return completionSource.Task;
        }
    }
}
