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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Admits downloads by listed size against 128 MiB in arrival order; a larger object waits until nothing is charged and runs alone.
    /// </summary>
    internal sealed class TransferAllowance
    {
        public const long DefaultCap = 128L << 20;

        private readonly object _lock = new object();
        private readonly LinkedList<Waiter> _waiters = new LinkedList<Waiter>();
        private long _charged;

        public TransferAllowance(long cap = DefaultCap)
        {
            Cap = cap;
        }

        public long Cap { get; }

        public long Charged
        {
            get
            {
                lock (_lock)
                {
                    return _charged;
                }
            }
        }

        private sealed class Waiter
        {
            public required long Bytes { get; init; }

            public TaskCompletionSource<IDisposable> Admitted { get; } = new TaskCompletionSource<IDisposable>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private bool Fits(long bytes) => bytes > Cap ? _charged == 0 : _charged + bytes <= Cap;

        public Task<IDisposable> AdmitAsync(long bytes, CancellationToken cancellationToken = default)
        {
            bytes = Math.Max(0, bytes);
            Waiter waiter;
            LinkedListNode<Waiter> node;
            lock (_lock)
            {
                if (_waiters.Count == 0 && Fits(bytes))
                {
                    _charged += bytes;
                    return Task.FromResult<IDisposable>(new Charge(this, bytes));
                }
                waiter = new Waiter() { Bytes = bytes };
                node = _waiters.AddLast(waiter);
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
                            _waiters.Remove(node);
                        }
                    }
                    if (removed)
                    {
                        waiter.Admitted.TrySetCanceled(cancellationToken);
                        // A cancelled head may have held back smaller waiters behind it
                        Release(0);
                    }
                });
                waiter.Admitted.Task.ContinueWith(_ => registration.Dispose(), TaskScheduler.Default);
            }
            return waiter.Admitted.Task;
        }

        private void Release(long bytes)
        {
            var admitted = new List<(Waiter Waiter, Charge Charge)>();
            lock (_lock)
            {
                _charged -= bytes;
                // Arrival order: the head is admitted first, and nothing passes it
                while (_waiters.First is { } first && Fits(first.Value.Bytes))
                {
                    _waiters.RemoveFirst();
                    _charged += first.Value.Bytes;
                    admitted.Add((first.Value, new Charge(this, first.Value.Bytes)));
                }
            }
            foreach (var (waiter, charge) in admitted)
            {
                if (!waiter.Admitted.TrySetResult(charge))
                {
                    charge.Dispose();
                }
            }
        }

        private sealed class Charge : IDisposable
        {
            private TransferAllowance? _owner;
            private readonly long _bytes;

            public Charge(TransferAllowance owner, long bytes)
            {
                _owner = owner;
                _bytes = bytes;
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _owner, null)?.Release(_bytes);
            }
        }
    }
}
