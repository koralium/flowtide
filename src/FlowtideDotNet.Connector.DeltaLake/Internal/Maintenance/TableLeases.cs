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

using Stowage;
using Stowage.Impl;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// One exclusive lease per table path, handed out in arrival order; a record goes when its last holder and waiter leave.
    /// </summary>
    internal sealed class TableLeases
    {
        private readonly object _lock = new object();
        private readonly Dictionary<string, Entry> _entries = new Dictionary<string, Entry>(StringComparer.Ordinal);

        private sealed class Entry
        {
            public bool Held;

            public LinkedList<TaskCompletionSource<IDisposable>> Waiters { get; } = new LinkedList<TaskCompletionSource<IDisposable>>();
        }

        // Lease records currently kept
        public int Count
        {
            get
            {
                lock (_lock)
                {
                    return _entries.Count;
                }
            }
        }

        public int Waiting
        {
            get
            {
                lock (_lock)
                {
                    return _entries.Values.Sum(x => x.Waiters.Count);
                }
            }
        }

        /// <summary>
        /// The table's key: on local disk its absolute native path, compared without case on Windows; elsewhere the normalized path.
        /// </summary>
        public static string KeyOf(IFileStorage storage, IOPath table)
        {
            if (storage is ILocalDiskFileStorage local)
            {
                var native = Path.TrimEndingDirectorySeparator(Path.GetFullPath(local.ToNativeLocalPath(table)));
                return OperatingSystem.IsWindows() ? native.ToUpperInvariant() : native;
            }
            var normalized = IOPath.Normalize(table.Full);
            return normalized.Length > 1 ? normalized.TrimEnd(IOPath.PathSeparator) : normalized;
        }

        public Task<IDisposable> AcquireAsync(string key, CancellationToken cancellationToken = default)
        {
            TaskCompletionSource<IDisposable> waiter;
            LinkedListNode<TaskCompletionSource<IDisposable>> node;
            lock (_lock)
            {
                if (!_entries.TryGetValue(key, out var entry))
                {
                    entry = new Entry();
                    _entries[key] = entry;
                }
                if (!entry.Held && entry.Waiters.Count == 0)
                {
                    entry.Held = true;
                    return Task.FromResult<IDisposable>(new Lease(this, key));
                }
                waiter = new TaskCompletionSource<IDisposable>(TaskCreationOptions.RunContinuationsAsynchronously);
                node = entry.Waiters.AddLast(waiter);
            }
            if (cancellationToken.CanBeCanceled)
            {
                var registration = cancellationToken.Register(() =>
                {
                    bool removed;
                    lock (_lock)
                    {
                        removed = node.List != null;
                        if (removed)
                        {
                            var entry = _entries[key];
                            entry.Waiters.Remove(node);
                            if (!entry.Held && entry.Waiters.Count == 0)
                            {
                                _entries.Remove(key);
                            }
                        }
                    }
                    if (removed)
                    {
                        waiter.TrySetCanceled(cancellationToken);
                    }
                });
                waiter.Task.ContinueWith(_ => registration.Dispose(), TaskScheduler.Default);
            }
            return waiter.Task;
        }

        private void Release(string key)
        {
            TaskCompletionSource<IDisposable>? next = null;
            lock (_lock)
            {
                var entry = _entries[key];
                if (entry.Waiters.First is { } first)
                {
                    entry.Waiters.RemoveFirst();
                    next = first.Value;
                }
                else
                {
                    _entries.Remove(key);
                }
            }
            if (next != null)
            {
                var lease = new Lease(this, key);
                if (!next.TrySetResult(lease))
                {
                    lease.Dispose();
                }
            }
        }

        private sealed class Lease : IDisposable
        {
            private TableLeases? _owner;
            private readonly string _key;

            public Lease(TableLeases owner, string key)
            {
                _owner = owner;
                _key = key;
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _owner, null)?.Release(_key);
            }
        }
    }
}
