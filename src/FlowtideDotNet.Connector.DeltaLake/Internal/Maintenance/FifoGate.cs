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
    /// Counted slots handed out in arrival order; a cancelled waiter leaves the queue.
    /// </summary>
    internal sealed class FifoGate
    {
        private readonly object _lock = new object();
        private readonly LinkedList<Waiter> _waiters = new LinkedList<Waiter>();
        private readonly TimeProvider _clock;
        private int _free;

        public FifoGate(int slots, TimeProvider clock)
        {
            if (slots < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(slots));
            }
            Slots = slots;
            _free = slots;
            _clock = clock;
        }

        public int Slots { get; }

        public int Running
        {
            get
            {
                lock (_lock)
                {
                    return Slots - _free;
                }
            }
        }

        public int QueueLength
        {
            get
            {
                lock (_lock)
                {
                    return _waiters.Count;
                }
            }
        }

        // How long the head of the queue has waited
        public TimeSpan OldestWait
        {
            get
            {
                lock (_lock)
                {
                    return _waiters.First is { } first ? _clock.GetUtcNow() - first.Value.Since : TimeSpan.Zero;
                }
            }
        }

        private sealed class Waiter
        {
            public required DateTimeOffset Since { get; init; }

            public TaskCompletionSource<IDisposable> Admitted { get; } = new TaskCompletionSource<IDisposable>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public Task<IDisposable> AcquireAsync(CancellationToken cancellationToken = default)
        {
            Waiter waiter;
            LinkedListNode<Waiter> node;
            lock (_lock)
            {
                if (_waiters.Count == 0 && _free > 0)
                {
                    _free--;
                    return Task.FromResult<IDisposable>(new Slot(this));
                }
                waiter = new Waiter() { Since = _clock.GetUtcNow() };
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
                    }
                });
                waiter.Admitted.Task.ContinueWith(_ => registration.Dispose(), TaskScheduler.Default);
            }
            return waiter.Admitted.Task;
        }

        private void Release()
        {
            Waiter? next = null;
            lock (_lock)
            {
                // A freed slot goes straight to the head of the queue
                if (_waiters.First is { } first)
                {
                    _waiters.RemoveFirst();
                    next = first.Value;
                }
                else
                {
                    _free++;
                }
            }
            if (next != null)
            {
                var slot = new Slot(this);
                if (!next.Admitted.TrySetResult(slot))
                {
                    slot.Dispose();
                }
            }
        }

        private sealed class Slot : IDisposable
        {
            private FifoGate? _owner;

            public Slot(FifoGate owner)
            {
                _owner = owner;
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _owner, null)?.Release();
            }
        }
    }
}
