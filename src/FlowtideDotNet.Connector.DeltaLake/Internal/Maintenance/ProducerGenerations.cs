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
    internal enum BarrierOutcome : byte
    {
        Drained,
        Busy,
        StuckProducer,
        Timeout,
        Exhausted,
        Cancelled
    }

    /// <summary>
    /// Per-table producer generations, keyed like the table leases; an entry lives while a handle, a live producer or a barrier needs it.
    /// </summary>
    internal sealed class ProducerGenerations
    {
        private readonly object _lock = new object();
        private readonly Dictionary<string, ProducerEntry> _entries = new Dictionary<string, ProducerEntry>(StringComparer.Ordinal);

        // Entries currently kept
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

        public ProducerHandle Acquire(string key)
        {
            lock (_lock)
            {
                if (!_entries.TryGetValue(key, out var entry))
                {
                    entry = new ProducerEntry(this, key);
                    _entries[key] = entry;
                }
                entry.Handles++;
                return new ProducerHandle(this, entry);
            }
        }

        internal void Release(ProducerEntry entry)
        {
            lock (_lock)
            {
                entry.Handles--;
                RemoveIfUnused(entry);
            }
        }

        // A producer or barrier finished on an entry no handle holds
        internal void Collect(ProducerEntry entry)
        {
            lock (_lock)
            {
                RemoveIfUnused(entry);
            }
        }

        private void RemoveIfUnused(ProducerEntry entry)
        {
            if (entry.Handles == 0 && entry.Unused && _entries.TryGetValue(entry.Key, out var current) && ReferenceEquals(current, entry))
            {
                _entries.Remove(entry.Key);
            }
        }
    }

    /// <summary>
    /// One holder's claim on a table's generations; producers enter through it and a sweep's barrier runs on it.
    /// </summary>
    internal sealed class ProducerHandle : IDisposable
    {
        private ProducerGenerations? _owner;

        public ProducerHandle(ProducerGenerations owner, ProducerEntry entry)
        {
            _owner = owner;
            Entry = entry;
        }

        public ProducerEntry Entry { get; }

        public string Key => Entry.Key;

        public ProducerToken Enter() => Entry.Enter();

        public void Dispose()
        {
            Interlocked.Exchange(ref _owner, null)?.Release(Entry);
        }
    }

    /// <summary>
    /// An entered producer's slot, released with Exit; default when there is no coordination.
    /// </summary>
    internal readonly struct ProducerToken
    {
        private readonly ProducerEntry? _entry;
        private readonly int _slot;

        public ProducerToken(ProducerEntry entry, long generation)
        {
            _entry = entry;
            _slot = (int)(generation & 1);
            Generation = generation;
        }

        // The generation entered
        public long Generation { get; }

        public void Exit() => _entry?.Exit(_slot);
    }

    /// <summary>
    /// A full long generation and one producer counter per generation parity; at most one barrier at a time.
    /// </summary>
    internal sealed class ProducerEntry
    {
        private readonly ProducerGenerations _owner;
        private readonly int[] _counts = new int[2];
        private long _generation;
        private TaskCompletionSource? _drain;
        private int _barrier;
        private int _handles;

        public ProducerEntry(ProducerGenerations owner, string key)
        {
            _owner = owner;
            Key = key;
        }

        public string Key { get; }

        // Written under the owner's lock
        internal int Handles
        {
            get => Volatile.Read(ref _handles);
            set => Volatile.Write(ref _handles, value);
        }

        // No producer counted and no barrier running
        internal bool Unused => Volatile.Read(ref _counts[0]) == 0 && Volatile.Read(ref _counts[1]) == 0 && Volatile.Read(ref _barrier) == 0;

        public long Generation => Interlocked.Read(ref _generation);

        // Producers counted in the slot of the generation's parity
        public int Live(long generation) => Volatile.Read(ref _counts[(int)(generation & 1)]);

        internal void SetGenerationForTests(long generation) => Interlocked.Exchange(ref _generation, generation);

        // Run between a producer's read and its increment, and between its increment and its recheck, for race tests
        internal Action? BeforeIncrementHookForTests { get; set; }

        internal Action? AfterIncrementHookForTests { get; set; }

        /// <summary>
        /// Enters the current generation; no output may be written before this returns.
        /// </summary>
        public ProducerToken Enter()
        {
            while (true)
            {
                var generation = Interlocked.Read(ref _generation);
                var slot = (int)(generation & 1);
                BeforeIncrementHookForTests?.Invoke();
                Interlocked.Increment(ref _counts[slot]);
                AfterIncrementHookForTests?.Invoke();
                if (generation == Interlocked.Read(ref _generation))
                {
                    return new ProducerToken(this, generation);
                }
                // A flip ran between the read and the increment
                if (Interlocked.Decrement(ref _counts[slot]) == 0)
                {
                    Signal();
                }
            }
        }

        public void Exit(int slot)
        {
            if (Interlocked.Decrement(ref _counts[slot]) == 0)
            {
                Signal();
                CollectIfUnheld();
            }
        }

        // An entry whose last handle went while it was in use leaves once that use ends
        private void CollectIfUnheld()
        {
            if (Handles == 0)
            {
                _owner.Collect(this);
            }
        }

        private void Signal()
        {
            Volatile.Read(ref _drain)?.TrySetResult();
        }

        // A full fence before the counts are read again
        private TaskCompletionSource Arm()
        {
            var drain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Interlocked.Exchange(ref _drain, drain);
            return drain;
        }

        /// <summary>
        /// Moves producers to the next generation and waits until none is left in the old one.
        /// </summary>
        public async Task<BarrierOutcome> FlipAndDrainAsync(TimeSpan timeout, TimeProvider clock, CancellationToken cancellationToken)
        {
            if (Interlocked.CompareExchange(ref _barrier, 1, 0) != 0)
            {
                return BarrierOutcome.Busy;
            }
            try
            {
                var generation = Interlocked.Read(ref _generation);
                if (generation == long.MaxValue)
                {
                    return BarrierOutcome.Exhausted;
                }
                var oldSlot = (int)(generation & 1);
                // A producer of two generations ago still counts in the destination slot
                if (Volatile.Read(ref _counts[oldSlot ^ 1]) != 0)
                {
                    return BarrierOutcome.StuckProducer;
                }
                // The notification is published before the flip, so no decrement to zero is missed
                var drain = Arm();
                Interlocked.Increment(ref _generation);
                var started = clock.GetTimestamp();
                while (Volatile.Read(ref _counts[oldSlot]) != 0)
                {
                    var remaining = timeout - clock.GetElapsedTime(started);
                    if (remaining <= TimeSpan.Zero)
                    {
                        return BarrierOutcome.Timeout;
                    }
                    try
                    {
                        await drain.Task.WaitAsync(remaining, clock, cancellationToken);
                    }
                    catch (TimeoutException)
                    {
                        // Live counts are never reset
                        return Volatile.Read(ref _counts[oldSlot]) == 0 ? BarrierOutcome.Drained : BarrierOutcome.Timeout;
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return BarrierOutcome.Cancelled;
                    }
                    drain = Arm();
                }
                return BarrierOutcome.Drained;
            }
            finally
            {
                Volatile.Write(ref _drain, null);
                Volatile.Write(ref _barrier, 0);
                CollectIfUnheld();
            }
        }
    }
}
