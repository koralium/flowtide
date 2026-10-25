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
    /// A token bucket that grants in arrival order; debt already incurred is repaid in turns and is never forgiven.
    /// </summary>
    internal sealed class FifoTokenBucket
    {
        private readonly object _lock = new object();
        private readonly LinkedList<Waiter> _waiters = new LinkedList<Waiter>();
        private readonly TimeProvider _clock;
        private readonly long _frequency;
        // Tokens times the timestamp frequency; below zero while a forfeited repayment is owed
        private Int128 _level;
        private long _stamp;
        // Incurred debt whose turns are not granted yet
        private long _pending;
        private long _charged;
        private long _largestTake;
        private ITimer? _timer;

        public FifoTokenBucket(long tokensPerSecond, long burst, TimeProvider clock)
        {
            if (tokensPerSecond < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(tokensPerSecond));
            }
            if (burst < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(burst));
            }
            Rate = tokensPerSecond;
            Burst = burst;
            _clock = clock;
            _frequency = clock.TimestampFrequency;
            _stamp = clock.GetTimestamp();
            _level = (Int128)burst * _frequency;
        }

        public long Rate { get; }

        public long Burst { get; }

        // Tokens taken or forfeited since creation
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

        public long Available
        {
            get
            {
                lock (_lock)
                {
                    RefillLocked();
                    return _level > 0 ? (long)(_level / _frequency) : 0;
                }
            }
        }

        /// <summary>
        /// Incurred debt not repaid yet: turns still queued and any forfeited remainder the bucket still owes.
        /// </summary>
        public long Debt
        {
            get
            {
                lock (_lock)
                {
                    RefillLocked();
                    return _pending + Owed();
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

        // No waiter, and more than the reserve on hand
        public bool HasSpare(long reserve)
        {
            lock (_lock)
            {
                RefillLocked();
                return _waiters.Count == 0 && _level > (Int128)reserve * _frequency;
            }
        }

        // How long the head of the queue has waited
        public TimeSpan OldestWait
        {
            get
            {
                lock (_lock)
                {
                    return _waiters.First is { } first ? _clock.GetElapsedTime(first.Value.Since) : TimeSpan.Zero;
                }
            }
        }

        // The largest single take, which never exceeds the burst
        internal long LargestTakeForTests
        {
            get
            {
                lock (_lock)
                {
                    return _largestTake;
                }
            }
        }

        private sealed class Waiter
        {
            public Waiter(long tokens, bool debt, long since)
            {
                Tokens = tokens;
                Debt = debt;
                Since = since;
            }

            public long Tokens { get; }

            public bool Debt { get; }

            // The clock's timestamp when it queued
            public long Since { get; }

            public TaskCompletionSource Granted { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>
        /// Takes tokens before the work they pay for; at most the burst at once, and never ahead of an earlier waiter.
        /// </summary>
        public ValueTask AcquireAsync(long tokens, CancellationToken cancellationToken = default) => AcquireCoreAsync(tokens, debt: false, cancellationToken);

        public void Acquire(long tokens) => AcquireAsync(tokens).AsTask().GetAwaiter().GetResult();

        /// <summary>
        /// Repays work already done in turns of at most the given size, each rejoining the queue's tail; a cancelled repayment leaves the rest owed by the bucket.
        /// </summary>
        public async Task RepayAsync(long debt, long turn, CancellationToken cancellationToken = default)
        {
            if (!Incur(debt, turn))
            {
                return;
            }
            var remaining = debt;
            try
            {
                while (remaining > 0)
                {
                    var take = Math.Min(remaining, turn);
                    await AcquireCoreAsync(take, debt: true, cancellationToken);
                    remaining -= take;
                }
            }
            catch
            {
                Forfeit(remaining);
                throw;
            }
        }

        // Blocks on each turn itself, so no continuation needs the caller's context
        public void Repay(long debt, long turn)
        {
            if (!Incur(debt, turn))
            {
                return;
            }
            var remaining = debt;
            try
            {
                while (remaining > 0)
                {
                    var take = Math.Min(remaining, turn);
                    AcquireCoreAsync(take, debt: true, default).AsTask().GetAwaiter().GetResult();
                    remaining -= take;
                }
            }
            catch
            {
                Forfeit(remaining);
                throw;
            }
        }

        /// <summary>
        /// Work already done, owed at once without a turn: a deficit every later take waits out.
        /// </summary>
        public void Owe(long debt)
        {
            if (Incur(debt, 1))
            {
                Forfeit(debt);
            }
        }

        // Records the debt as owed; false when there is none
        private bool Incur(long debt, long turn)
        {
            if (debt <= 0)
            {
                return false;
            }
            if (turn < 1 || turn > Burst)
            {
                throw new ArgumentOutOfRangeException(nameof(turn));
            }
            lock (_lock)
            {
                _pending += debt;
            }
            return true;
        }

        private ValueTask AcquireCoreAsync(long tokens, bool debt, CancellationToken cancellationToken)
        {
            if (tokens < 1 || tokens > Burst)
            {
                throw new ArgumentOutOfRangeException(nameof(tokens), tokens, $"A take is between 1 and the burst of {Burst}");
            }
            if (cancellationToken.IsCancellationRequested)
            {
                return ValueTask.FromCanceled(cancellationToken);
            }
            Waiter? waiter = null;
            LinkedListNode<Waiter>? node = null;
            List<Waiter>? granted;
            lock (_lock)
            {
                RefillLocked();
                // A late timer must not hold an arrival behind a head that can already go
                granted = GrantLocked();
                if (_waiters.Count == 0 && _level >= (Int128)tokens * _frequency)
                {
                    TakeLocked(tokens, debt);
                }
                else
                {
                    waiter = new Waiter(tokens, debt, _clock.GetTimestamp());
                    node = _waiters.AddLast(waiter);
                    ScheduleLocked();
                }
            }
            Complete(granted);
            if (waiter == null || node == null)
            {
                return ValueTask.CompletedTask;
            }
            if (cancellationToken.CanBeCanceled)
            {
                var registration = cancellationToken.Register(() => Cancel(node, cancellationToken));
                waiter.Granted.Task.ContinueWith(_ => registration.Dispose(), TaskScheduler.Default);
            }
            return new ValueTask(waiter.Granted.Task);
        }

        private void Cancel(LinkedListNode<Waiter> node, CancellationToken cancellationToken)
        {
            bool removed;
            List<Waiter>? granted = null;
            lock (_lock)
            {
                removed = node.List != null;
                if (removed)
                {
                    var head = node == _waiters.First;
                    _waiters.Remove(node);
                    // A cancelled head may have held back the waiters behind it
                    if (head)
                    {
                        granted = GrantLocked();
                        ScheduleLocked();
                    }
                }
            }
            if (removed)
            {
                node.Value.Granted.TrySetCanceled(cancellationToken);
            }
            Complete(granted);
        }

        // The unpaid rest becomes a deficit every later take waits out
        private void Forfeit(long remaining)
        {
            if (remaining <= 0)
            {
                return;
            }
            lock (_lock)
            {
                RefillLocked();
                _pending -= remaining;
                _level -= (Int128)remaining * _frequency;
                _charged += remaining;
                ScheduleLocked();
            }
        }

        private void TakeLocked(long tokens, bool debt)
        {
            _level -= (Int128)tokens * _frequency;
            _charged += tokens;
            _largestTake = Math.Max(_largestTake, tokens);
            if (debt)
            {
                _pending -= tokens;
            }
        }

        private long Owed() => _level < 0 ? (long)((-_level + _frequency - 1) / _frequency) : 0;

        private void RefillLocked()
        {
            var now = _clock.GetTimestamp();
            var elapsed = now - _stamp;
            if (elapsed <= 0)
            {
                return;
            }
            _stamp = now;
            var full = (Int128)Burst * _frequency;
            if (_level < full)
            {
                _level = Int128.Min(full, _level + (Int128)elapsed * Rate);
            }
        }

        // The queue's head first, and nothing passes it
        private List<Waiter>? GrantLocked()
        {
            if (_waiters.Count == 0)
            {
                return null;
            }
            RefillLocked();
            List<Waiter>? granted = null;
            while (_waiters.First is { } first && _level >= (Int128)first.Value.Tokens * _frequency)
            {
                _waiters.RemoveFirst();
                TakeLocked(first.Value.Tokens, first.Value.Debt);
                (granted ??= new List<Waiter>()).Add(first.Value);
            }
            return granted;
        }

        // One timer, due when the head's tokens have accrued
        private void ScheduleLocked()
        {
            if (_waiters.First is not { } first)
            {
                _timer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                return;
            }
            var missing = (Int128)first.Value.Tokens * _frequency - _level;
            var stamps = missing <= 0 ? 0 : (missing + Rate - 1) / Rate;
            var ticks = (stamps * TimeSpan.TicksPerSecond + _frequency - 1) / _frequency;
            var due = TimeSpan.FromTicks((long)Int128.Clamp(ticks, 1, TimeSpan.MaxValue.Ticks / 2));
            if (_timer == null)
            {
                // The timer captures no caller context
                if (ExecutionContext.IsFlowSuppressed())
                {
                    _timer = _clock.CreateTimer(_ => OnTimer(), null, due, Timeout.InfiniteTimeSpan);
                }
                else
                {
                    using (ExecutionContext.SuppressFlow())
                    {
                        _timer = _clock.CreateTimer(_ => OnTimer(), null, due, Timeout.InfiniteTimeSpan);
                    }
                }
                return;
            }
            _timer.Change(due, Timeout.InfiniteTimeSpan);
        }

        private void OnTimer()
        {
            List<Waiter>? granted;
            lock (_lock)
            {
                granted = GrantLocked();
                ScheduleLocked();
            }
            Complete(granted);
        }

        // Outside the lock; a granted waiter was removed under it, so its cancellation can no longer win
        private static void Complete(List<Waiter>? granted)
        {
            if (granted == null)
            {
                return;
            }
            foreach (var waiter in granted)
            {
                waiter.Granted.TrySetResult();
            }
        }
    }
}
