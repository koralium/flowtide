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

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // Connects a row store's host with its fetches
    internal sealed class RowStoreHandle
    {
        private readonly object _lock = new object();
        private TaskCompletionSource<RowStoreHost> _opened = NewSource();
        private TaskCompletionSource? _idle;
        private int _generation;
        private int _reads;

        public RowStoreHandle(string name)
        {
            Name = name;
        }

        public string Name { get; }

        // Set at build, a store nobody hosts is refused there
        public bool HasHost { get; set; }

        // Goes up each time the host opens its store
        public int Generation => Volatile.Read(ref _generation);

        private static TaskCompletionSource<RowStoreHost> NewSource()
        {
            return new TaskCompletionSource<RowStoreHost>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        // The host has opened its store, fetches may read it
        public void Open(RowStoreHost host)
        {
            lock (_lock)
            {
                if (_opened.Task.IsCompleted)
                {
                    _opened = NewSource();
                }
                _generation++;
                _opened.SetResult(host);
            }
        }

        // No new read starts, the running ones end first
        public Task CloseAsync()
        {
            lock (_lock)
            {
                if (_opened.Task.IsCompleted)
                {
                    _opened = NewSource();
                    _generation++;
                }
                if (_reads == 0)
                {
                    return Task.CompletedTask;
                }
                _idle ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return _idle.Task;
            }
        }

        // False when the store closed or opened anew
        public bool TryBeginRead(int generation)
        {
            lock (_lock)
            {
                if (!_opened.Task.IsCompleted || _generation != generation)
                {
                    return false;
                }
                _reads++;
                return true;
            }
        }

        public void EndRead()
        {
            lock (_lock)
            {
                _reads--;
                if (_reads == 0 && _idle != null)
                {
                    _idle.SetResult();
                    _idle = null;
                }
            }
        }

        // The open store, a fetch waits here for its host
        public Task<RowStoreHost> GetHostAsync(CancellationToken cancellationToken)
        {
            Task<RowStoreHost> task;
            lock (_lock)
            {
                task = _opened.Task;
            }
            return task.IsCompleted ? task : task.WaitAsync(cancellationToken);
        }
    }

    // An operator that can keep columns in a row store
    internal interface IRowStoreHostVertex
    {
        void AttachRowStoreHandle(RowStoreHandle handle);
    }
}
