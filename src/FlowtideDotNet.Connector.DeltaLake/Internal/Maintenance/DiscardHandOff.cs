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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// How a sink run handles the outputs an attempt discards, chosen once per run.
    /// </summary>
    internal enum DiscardMode : byte
    {
        // The attempt deletes them itself before it stages
        Inline,
        // The attempt hands them to the worker after it stages
        Queued
    }

    /// <summary>
    /// Where a table's discard hints go: the storage instance below connector-owned wrappers, compared by reference, and the table's lease key on it.
    /// </summary>
    internal readonly struct DiscardRoute : IEquatable<DiscardRoute>
    {
        public DiscardRoute(IFileStorage raw, string key)
        {
            Raw = raw;
            Key = key;
        }

        public IFileStorage Raw { get; }

        public string Key { get; }

        /// <summary>
        /// The route of a table on a storage, metered or not; throws when the table's key cannot be formed.
        /// </summary>
        public static DiscardRoute Of(IFileStorage storage, IOPath table)
        {
            var raw = RawStorage.Unwrap(storage);
            return new DiscardRoute(raw, TableLeases.KeyOf(raw, table));
        }

        public bool Equals(DiscardRoute other) => ReferenceEquals(Raw, other.Raw) && string.Equals(Key, other.Key, StringComparison.Ordinal);

        public override bool Equals(object? obj) => obj is DiscardRoute other && Equals(other);

        public override int GetHashCode() => HashCode.Combine(RuntimeHelpers.GetHashCode(Raw), StringComparer.Ordinal.GetHashCode(Key));
    }

    /// <summary>
    /// What a drain turn concluded for one hint it took.
    /// </summary>
    internal enum HintState : byte
    {
        // Not concluded: the hint stays in place
        Open,
        // Deleted, absent, kept or left behind: the hint goes
        Settled,
        // Its delete failed: the hint waits for its retry with one more failed turn
        Failed
    }

    /// <summary>
    /// One attempt's discarded outputs, each a hint: the table id, epoch, version and stage id they were staged under, and their table-relative paths.
    /// </summary>
    internal sealed class DiscardGroup
    {
        public DiscardGroup(string? tableId, long adoptedAt, long version, string stageId, IReadOnlyList<string> paths)
        {
            TableId = tableId;
            AdoptedAt = adoptedAt;
            Version = version;
            StageId = stageId;
            Paths = paths;
        }

        public string? TableId { get; }

        public long AdoptedAt { get; }

        public long Version { get; }

        public string StageId { get; }

        // All the attempt's paths until queued, then only the queued paths not yet settled, in turn order
        public IReadOnlyList<string> Paths { get; private set; }

        // The producing sink's counters, for drops after the route's last handle settled
        public DiscardCounters? Counters { get; init; }

        // When the attempt queued it, on the options clock; unset reads as queued long ago
        public DateTimeOffset QueuedAt { get; init; }

        // The queued paths not yet settled, each charged to the allowance
        internal int Admitted;

        // Bytes still charged to the allowance
        internal long Charged;

        // Each path's failed turns and retry time, by its index in Paths; null while none failed
        private Retry[]? _retries;

        // Keeps since the group last passed its checks, and when a turn may take it again
        private Retry _gate;

        public int Remaining => Admitted;

        // The turns that kept it since it last passed its checks; never counted as failed deletes
        internal int KeptTurns => _gate.Turns;

        // Kept until its retry, at least the floor away, outside every due check and turn; drainer only
        internal void Keep(DateTimeOffset now, TimeSpan floor)
        {
            var turns = Math.Min(_gate.Turns + 1, byte.MaxValue);
            var delay = DiscardHandOff.RetryDelay(turns);
            _gate = new Retry((byte)turns, now + (floor > delay ? floor : delay));
        }

        // Passed its checks: kept no more; drainer only
        internal void Passed() => _gate = default;

        // The turns in which the delete of the path at the index failed
        internal int FailedTurns(int index) => RetryOf(index).Turns;

        // When the path at the index may be tried again, on the options clock
        internal DateTimeOffset RetryAt(int index) => RetryOf(index).At;

        // When its first path may be tried: the later of the group's own retry and its first path's, which is its paths' earliest
        internal DateTimeOffset ReadyAt => Remaining == 0 ? DateTimeOffset.MinValue : RetryOf(0).At > _gate.At ? RetryOf(0).At : _gate.At;

        // How many of the next paths a turn may take at the time, at most max; paths stay in retry order, so the due ones lead
        internal int Due(DateTimeOffset now, int max)
        {
            var limit = Math.Min(max, Remaining);
            if (_gate.At > now)
            {
                return 0;
            }
            if (_retries == null)
            {
                return limit;
            }
            var due = 0;
            while (due < limit && _retries[due].At <= now)
            {
                due++;
            }
            return due;
        }

        private Retry RetryOf(int index) => _retries?[index] ?? default;

        // Drops the references to paths that were not charged
        internal void Truncate(int count)
        {
            var kept = new string[count];
            for (int i = 0; i < count; i++)
            {
                kept[i] = Paths[i];
            }
            Paths = kept;
        }

        // Drops the references to the first paths, which settled, before their charge goes; returns their bytes
        internal long Release(int count)
        {
            long bytes = 0;
            for (int i = 0; i < count; i++)
            {
                bytes += DiscardHandOff.PathBytes(Paths[i]);
            }
            var remaining = Remaining - count;
            if (remaining == 0)
            {
                Paths = Array.Empty<string>();
                _retries = null;
            }
            else
            {
                var paths = new string[remaining];
                for (int i = 0; i < remaining; i++)
                {
                    paths[i] = Paths[count + i];
                }
                if (_retries != null)
                {
                    var retries = new Retry[remaining];
                    Array.Copy(_retries, count, retries, 0, remaining);
                    _retries = retries;
                }
                Paths = paths;
            }
            Admitted = remaining;
            return bytes;
        }

        // Keeps the next paths' open ones in order and puts each failed one, one failed turn more, behind every path due no later; returns the settled ones' bytes
        internal long Rearrange(ReadOnlySpan<HintState> states, DateTimeOffset now)
        {
            var count = Remaining;
            var retrying = new List<(string Path, Retry Retry)>();
            for (int i = 0; i < states.Length; i++)
            {
                if (states[i] == HintState.Settled)
                {
                    count--;
                }
                else if (states[i] == HintState.Failed)
                {
                    var turns = Math.Min(FailedTurns(i) + 1, byte.MaxValue);
                    retrying.Add((Paths[i], new Retry((byte)turns, now + DiscardHandOff.RetryDelay(turns))));
                }
            }
            // Stable, so failures due together keep their order
            var failed = retrying.OrderBy(x => x.Retry.At).ToList();
            var paths = new string[count];
            var retries = new Retry[count];
            var any = false;
            long settled = 0;
            var kept = 0;
            var placed = 0;
            for (int i = 0; i < Remaining; i++)
            {
                var state = i < states.Length ? states[i] : HintState.Open;
                if (state == HintState.Settled)
                {
                    settled += DiscardHandOff.PathBytes(Paths[i]);
                }
                else if (state == HintState.Open)
                {
                    var retry = RetryOf(i);
                    while (placed < failed.Count && failed[placed].Retry.At < retry.At)
                    {
                        Keep(failed[placed++]);
                    }
                    Keep((Paths[i], retry));
                }
            }
            while (placed < failed.Count)
            {
                Keep(failed[placed++]);
            }
            Paths = paths;
            _retries = any ? retries : null;
            Admitted = count;
            return settled;

            void Keep((string Path, Retry Retry) item)
            {
                paths[kept] = item.Path;
                retries[kept] = item.Retry;
                any |= item.Retry.Turns > 0;
                kept++;
            }
        }

        // A path's failed turns and the time before which it is not tried again
        private readonly record struct Retry(byte Turns, DateTimeOffset At);
    }

    /// <summary>
    /// Per-sink counts of discarded outputs, from both the attempt and the worker.
    /// </summary>
    internal sealed class DiscardCounters
    {
        private long _queued;
        private long _dropped;
        private long _deleted;
        private long _absent;
        private long _kept;
        private long _failed;
        private long _leaked;

        public long Queued => Interlocked.Read(ref _queued);

        public long Dropped => Interlocked.Read(ref _dropped);

        public long Deleted => Interlocked.Read(ref _deleted);

        public long Absent => Interlocked.Read(ref _absent);

        public long Kept => Interlocked.Read(ref _kept);

        public long Failed => Interlocked.Read(ref _failed);

        // Hints whose deletes kept failing until they were left behind
        public long Leaked => Interlocked.Read(ref _leaked);

        public void AddQueued(long count) => Interlocked.Add(ref _queued, count);

        public void AddDropped(long count) => Interlocked.Add(ref _dropped, count);

        public void Add(DeleteOutcome outcome)
        {
            switch (outcome.Status)
            {
                case DeleteStatus.Deleted:
                    Interlocked.Increment(ref _deleted);
                    break;
                case DeleteStatus.Absent:
                    Interlocked.Increment(ref _absent);
                    break;
                case DeleteStatus.Kept:
                    Interlocked.Increment(ref _kept);
                    break;
                default:
                    Interlocked.Increment(ref _failed);
                    break;
            }
        }

        public void Add(DeletionTally tally)
        {
            Interlocked.Add(ref _deleted, tally.Deleted);
            Interlocked.Add(ref _absent, tally.Absent);
            Interlocked.Add(ref _kept, tally.Kept);
            Interlocked.Add(ref _failed, tally.Failed);
            Interlocked.Add(ref _leaked, tally.Leaked);
        }
    }

    /// <summary>
    /// The process-wide discard hand-off: hints routed by storage instance and table and charged to one retained-memory allowance; lossy by design, so overflow drops and counts.
    /// </summary>
    internal sealed class DiscardHandOff
    {
        public const long DefaultCapacity = 4L << 20;

        // Estimated managed bytes of a group beside its id strings' characters: the group, its strings' and arrays' headers and its queue slots
        internal const long GroupOverheadBytes = 256;
        // Estimated managed bytes of a queued path beside its characters: its string header, its slot in Paths and its slot in the retries
        internal const long PathOverheadBytes = 56;

        public static readonly TimeSpan DefaultDrainSpacing = TimeSpan.FromMinutes(5);

        private static readonly TimeSpan s_retryBase = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan s_retryCap = TimeSpan.FromMinutes(30);

        private readonly object _lock = new object();
        private readonly ConcurrentDictionary<DiscardRoute, DiscardEntry> _entries = new ConcurrentDictionary<DiscardRoute, DiscardEntry>();
        private long _charged;
        private long _held;
        private long _queued;
        private long _dropped;
        private long _leaked;
        // Routes held whole since their last turn declined, replaced whole under the lock
        private DiscardEntry[] _holding = Array.Empty<DiscardEntry>();

        public DiscardHandOff(long capacity = DefaultCapacity)
        {
            Capacity = capacity;
        }

        public long Capacity { get; }

        // The most one route holds, so a route whose hints never drain leaves room for the others
        public long RouteCapacity => Capacity / 2;

        // How long a route's hints wait on the options clock before a turn drains them, unless enough queue up or the allowance runs short
        public TimeSpan DrainSpacing { get; set; } = DefaultDrainSpacing;

        // Drainable hints fill three quarters of the room the held ones leave
        public bool UnderPressure
        {
            get
            {
                var held = Held;
                var room = Capacity - held;
                return Charged - held >= room - room / 4;
            }
        }

        // Retained bytes charged by queued hints
        public long Charged => Interlocked.Read(ref _charged);

        // Bytes of the hints each route's drainer last saw above its listed head or kept for a later turn, and every byte of a held route
        public long Held
        {
            get
            {
                var held = Interlocked.Read(ref _held);
                foreach (var entry in Volatile.Read(ref _holding))
                {
                    held += entry.Charged;
                }
                return held;
            }
        }

        // Hints queued across every route
        public long Queued => Interlocked.Read(ref _queued);

        // Hints dropped without a delete, across every route
        public long Dropped => Interlocked.Read(ref _dropped);

        // Hints left behind after their deletes failed in every turn, across every route
        public long Leaked => Interlocked.Read(ref _leaked);

        // Routes some handle holds
        public int Routes => _entries.Count;

        // Called with every group a producer queues
        internal Action<DiscardGroup>? EnqueuedHookForTests { get; set; }

        /// <summary>
        /// A handle on the route's entry; a route no handle holds starts empty.
        /// </summary>
        public DiscardHandle Acquire(DiscardRoute route)
        {
            lock (_lock)
            {
                if (!_entries.TryGetValue(route, out var entry))
                {
                    entry = new DiscardEntry(this, route);
                    _entries[route] = entry;
                }
                entry.Handles++;
                return new DiscardHandle(entry);
            }
        }

        /// <summary>
        /// The route's entry while a handle holds it; lock-free, for a worker's IsDue.
        /// </summary>
        public DiscardEntry? Find(DiscardRoute route) => _entries.TryGetValue(route, out var entry) ? entry : null;

        /// <summary>
        /// Another handle on an entry still held; false once its last handle settled.
        /// </summary>
        public bool TryAcquire(DiscardEntry entry, out DiscardHandle? handle)
        {
            lock (_lock)
            {
                if (entry.Released)
                {
                    handle = null;
                    return false;
                }
                entry.Handles++;
                handle = new DiscardHandle(entry);
                return true;
            }
        }

        /// <summary>
        /// Drops the route's queued hints without a delete when no drain runs, for a run that cannot drain them; returns the hints dropped.
        /// </summary>
        public int DropQueued(DiscardRoute route)
        {
            if (Find(route) is not { } entry || !TryAcquire(entry, out var handle))
            {
                return 0;
            }
            using (handle)
            {
                if (!entry.TryBeginDrain())
                {
                    // The running drain keeps its handle until it settles
                    return 0;
                }
                try
                {
                    return entry.DropAll();
                }
                finally
                {
                    entry.EndDrain();
                }
            }
        }

        internal void Release(DiscardEntry entry)
        {
            lock (_lock)
            {
                if (--entry.Handles > 0)
                {
                    return;
                }
                entry.Released = true;
                _entries.TryRemove(new KeyValuePair<DiscardRoute, DiscardEntry>(entry.Route, entry));
            }
            // No producer or drainer holds the entry any more
            entry.DropReleased();
        }

        internal static long GroupBytes(DiscardGroup group) => GroupOverheadBytes + 2L * ((group.TableId?.Length ?? 0) + group.StageId.Length);

        internal static long PathBytes(string path) => PathOverheadBytes + 2L * path.Length;

        // How long a hint waits after its delete failed in the given number of turns: one minute doubling, at most thirty
        internal static TimeSpan RetryDelay(int failedTurns) => TimeSpan.FromTicks(Math.Min(s_retryBase.Ticks << Math.Clamp(failedTurns - 1, 0, 5), s_retryCap.Ticks));

        // False, charging nothing, when the bytes do not fit
        internal bool TryCharge(long bytes)
        {
            var charged = Interlocked.Read(ref _charged);
            while (true)
            {
                if (charged + bytes > Capacity)
                {
                    return false;
                }
                var seen = Interlocked.CompareExchange(ref _charged, charged + bytes, charged);
                if (seen == charged)
                {
                    return true;
                }
                charged = seen;
            }
        }

        internal void Uncharge(long bytes) => Interlocked.Add(ref _charged, -bytes);

        internal void AddHeld(long bytes) => Interlocked.Add(ref _held, bytes);

        internal void Hold(DiscardEntry entry)
        {
            lock (_lock)
            {
                Volatile.Write(ref _holding, _holding.Append(entry).ToArray());
            }
        }

        internal void Unhold(DiscardEntry entry)
        {
            lock (_lock)
            {
                Volatile.Write(ref _holding, Array.FindAll(_holding, x => !ReferenceEquals(x, entry)));
            }
        }

        internal void AddQueued(long hints) => Interlocked.Add(ref _queued, hints);

        internal void AddDropped(long hints) => Interlocked.Add(ref _dropped, hints);

        internal void AddLeaked(long hints) => Interlocked.Add(ref _leaked, hints);
    }

    /// <summary>
    /// One holder's claim on a route: a sink run producing hints, or a drain servicing them until it settles.
    /// </summary>
    internal sealed class DiscardHandle : IDisposable
    {
        private DiscardEntry? _held;

        public DiscardHandle(DiscardEntry entry)
        {
            Entry = entry;
            _held = entry;
        }

        public DiscardEntry Entry { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _held, null) is { } entry)
            {
                entry.Owner.Release(entry);
            }
        }
    }

    /// <summary>
    /// A commit whose publish failed closed, by the version and stage id its hints carry.
    /// </summary>
    internal sealed record FailedClosedMark(long Version, string StageId);

    /// <summary>
    /// One route's queued hints; producers add through a handle without a lock, and one drainer at a time services them.
    /// </summary>
    internal sealed class DiscardEntry
    {
        private readonly ConcurrentQueue<DiscardGroup> _inbox = new ConcurrentQueue<DiscardGroup>();
        // In queue order; touched only by the drainer, or after the last handle
        private readonly List<DiscardGroup> _pending = new List<DiscardGroup>();
        // The pending groups as the drainer last left them, for the worker's due check
        private Pended[] _view = Array.Empty<Pended>();
        // Hints queued since the drainer last took the inbox, their oldest stamp in ticks and their lowest version; at least what the inbox holds
        private long _inboxHints;
        private long _inboxOldest = long.MaxValue;
        private long _inboxLowest = long.MaxValue;
        private FailedClosedMark? _failedClosed;
        // The head of the drainer's last listing, and the bytes it holds back from the allowance's pressure; drainer only
        private long _listedHead = long.MaxValue;
        private long _held;
        // Held whole since a turn declined, until the next turn starts; drainer only
        private bool _holding;
        private int _hints;
        private long _charged;
        private int _draining;
        private volatile bool _released;

        public DiscardEntry(DiscardHandOff owner, DiscardRoute route)
        {
            Owner = owner;
            Route = route;
        }

        public DiscardHandOff Owner { get; }

        public DiscardRoute Route { get; }

        // Changed under the owner's lock
        internal int Handles;

        // Set under the owner's lock once the last handle settled
        internal bool Released
        {
            get => _released;
            set => _released = value;
        }

        // Queued hints not yet settled or dropped
        public int Hints => Volatile.Read(ref _hints);

        // Bytes this route holds of the allowance
        public long Charged => Interlocked.Read(ref _charged);

        // The commit whose hints the next turn drops, null when none waits
        public FailedClosedMark? FailedClosed => Volatile.Read(ref _failedClosed);

        public void MarkFailedClosed(FailedClosedMark mark) => Volatile.Write(ref _failedClosed, mark);

        // Cleared only while it is still the given mark
        public bool ClearFailedClosed(FailedClosedMark mark) => ReferenceEquals(Interlocked.CompareExchange(ref _failedClosed, null, mark), mark);

        // The queued groups in order, read while no drain runs
        internal IReadOnlyList<DiscardGroup> QueuedForTests => _pending.Concat(_inbox).Where(x => x.Remaining > 0).ToList();

        /// <summary>
        /// Charges and queues an attempt's paths through a held handle; the paths that do not fit the allowance or the route's share are dropped and counted, and their number is returned.
        /// </summary>
        public int Enqueue(DiscardGroup group)
        {
            var paths = group.Paths;
            Owner.EnqueuedHookForTests?.Invoke(group);
            if (paths.Count == 0)
            {
                return 0;
            }
            var groupBytes = DiscardHandOff.GroupBytes(group);
            if (_released || !TryCharge(groupBytes))
            {
                Owner.AddDropped(paths.Count);
                return paths.Count;
            }
            var charged = groupBytes;
            var admitted = 0;
            while (admitted < paths.Count)
            {
                var bytes = DiscardHandOff.PathBytes(paths[admitted]);
                if (!TryCharge(bytes))
                {
                    break;
                }
                charged += bytes;
                admitted++;
            }
            var dropped = paths.Count - admitted;
            if (dropped > 0)
            {
                Owner.AddDropped(dropped);
            }
            if (admitted == 0)
            {
                Uncharge(charged);
                return dropped;
            }
            if (dropped > 0)
            {
                group.Truncate(admitted);
            }
            group.Admitted = admitted;
            group.Charged = charged;
            Interlocked.Add(ref _hints, admitted);
            Owner.AddQueued(admitted);
            _inbox.Enqueue(group);
            // Summed after the inbox holds the group
            Interlocked.Add(ref _inboxHints, admitted);
            LowerTo(ref _inboxLowest, group.Version);
            LowerTo(ref _inboxOldest, group.QueuedAt.UtcTicks);
            return dropped;
        }

        // A read, and a compare-exchange only when the value is lower
        private static void LowerTo(ref long location, long value)
        {
            var seen = Volatile.Read(ref location);
            while (value < seen)
            {
                var current = Interlocked.CompareExchange(ref location, value, seen);
                if (current == seen)
                {
                    return;
                }
                seen = current;
            }
        }

        /// <summary>
        /// Whether a turn should drain the route: its drainable hints fill a batch, the oldest waited the spacing, the allowance runs short, or a failed-closed mark waits; reads the drainer's view and the inbox summary, never the inbox.
        /// </summary>
        public bool IsDue(long head, DateTimeOffset now, int batch)
        {
            if (FailedClosed != null && Hints > 0)
            {
                return true;
            }
            long hints = 0;
            var oldest = DateTimeOffset.MaxValue;
            foreach (var pended in Volatile.Read(ref _view))
            {
                if (pended.Version <= head && pended.ReadyAt <= now)
                {
                    hints += pended.Hints;
                    oldest = pended.QueuedAt < oldest ? pended.QueuedAt : oldest;
                }
            }
            var inbox = Interlocked.Read(ref _inboxHints);
            // Every inbox hint counts once the inbox's lowest version is at or below the head, never on a route without hints
            if (inbox > 0 && Hints > 0 && Interlocked.Read(ref _inboxLowest) <= head)
            {
                hints += inbox;
                var stamp = new DateTimeOffset(Math.Min(Interlocked.Read(ref _inboxOldest), DateTimeOffset.MaxValue.UtcTicks), TimeSpan.Zero);
                oldest = stamp < oldest ? stamp : oldest;
            }
            return hints > 0 && (hints >= batch || now - oldest >= Owner.DrainSpacing || Owner.UnderPressure);
        }

        internal bool TryBeginDrain() => Interlocked.CompareExchange(ref _draining, 1, 0) == 0;

        internal void EndDrain()
        {
            Publish();
            Volatile.Write(ref _draining, 0);
        }

        /// <summary>
        /// The queued groups in order, newly queued ones last; drainer only.
        /// </summary>
        internal IReadOnlyList<DiscardGroup> Pending()
        {
            // Reset before the inbox is taken
            Interlocked.Exchange(ref _inboxHints, 0);
            Interlocked.Exchange(ref _inboxLowest, long.MaxValue);
            Interlocked.Exchange(ref _inboxOldest, long.MaxValue);
            while (_inbox.TryDequeue(out var group))
            {
                _pending.Add(group);
            }
            // The turn starts: the groups' own holds replace the route's
            var holding = _holding;
            _holding = false;
            Publish();
            if (holding)
            {
                Owner.Unhold(this);
            }
            return _pending;
        }

        // The head the drainer's listing showed, above which its hints are held; drainer only
        internal void Listed(long head) => _listedHead = head;

        /// <summary>
        /// Holds every byte the route charges back from the pressure, the inbox included, until its next turn starts or its last handle settles; drainer only.
        /// </summary>
        internal void Hold()
        {
            if (!_holding)
            {
                _holding = true;
                Owner.Hold(this);
            }
        }

        // The pending groups' due facts for the worker, replaced whole, and the bytes held back from the pressure; drainer only
        private void Publish()
        {
            var view = new List<Pended>(_pending.Count);
            long held = 0;
            foreach (var group in _pending)
            {
                if (group.Remaining > 0)
                {
                    view.Add(new Pended(group.Version, group.Remaining, group.QueuedAt, group.ReadyAt));
                    // A held route's bytes are all held through the owner
                    if (!_holding && (group.Version > _listedHead || group.KeptTurns > 0))
                    {
                        held += group.Charged;
                    }
                }
            }
            Volatile.Write(ref _view, view.ToArray());
            Owner.AddHeld(held - _held);
            _held = held;
        }

        // A route takes at most its share of the allowance
        private bool TryCharge(long bytes)
        {
            var charged = Interlocked.Read(ref _charged);
            while (true)
            {
                if (charged + bytes > Owner.RouteCapacity)
                {
                    return false;
                }
                var seen = Interlocked.CompareExchange(ref _charged, charged + bytes, charged);
                if (seen == charged)
                {
                    break;
                }
                charged = seen;
            }
            if (Owner.TryCharge(bytes))
            {
                return true;
            }
            Interlocked.Add(ref _charged, -bytes);
            return false;
        }

        private void Uncharge(long bytes)
        {
            Interlocked.Add(ref _charged, -bytes);
            Owner.Uncharge(bytes);
        }

        /// <summary>
        /// Settles a group's next paths, dropping their references before their charge, and the group's own charge once none remain; drainer only.
        /// </summary>
        internal void Settle(DiscardGroup group, int count)
        {
            count = Math.Min(count, group.Remaining);
            if (count <= 0)
            {
                return;
            }
            var bytes = group.Release(count);
            if (group.Remaining == 0)
            {
                // The group's own charge goes with its last path
                bytes = group.Charged;
            }
            group.Charged -= bytes;
            Uncharge(bytes);
            Interlocked.Add(ref _hints, -count);
            Owner.AddQueued(-count);
        }

        /// <summary>
        /// Concludes a turn's take of a group's next hints at the time: settled ones go, failed ones wait for their retry and their group goes behind the others; returns the hints settled; drainer only.
        /// </summary>
        internal int Conclude(DiscardGroup group, ReadOnlySpan<HintState> states, DateTimeOffset now)
        {
            var prefix = 0;
            while (prefix < states.Length && states[prefix] == HintState.Settled)
            {
                prefix++;
            }
            var settled = prefix;
            var failed = false;
            for (int i = prefix; i < states.Length; i++)
            {
                settled += states[i] == HintState.Settled ? 1 : 0;
                failed |= states[i] == HintState.Failed;
            }
            if (!failed && settled == prefix)
            {
                // Settled in order, as most turns end
                Settle(group, prefix);
                return prefix;
            }
            var bytes = group.Rearrange(states, now);
            if (group.Remaining == 0)
            {
                bytes = group.Charged;
            }
            group.Charged -= bytes;
            Uncharge(bytes);
            Interlocked.Add(ref _hints, -settled);
            Owner.AddQueued(-settled);
            if (failed && _pending.Remove(group))
            {
                _pending.Add(group);
            }
            return settled;
        }

        /// <summary>
        /// Drops what remains of a group without a delete; returns the hints dropped; drainer only.
        /// </summary>
        internal int Drop(DiscardGroup group)
        {
            var remaining = group.Remaining;
            if (remaining <= 0)
            {
                return 0;
            }
            Settle(group, remaining);
            Owner.AddDropped(remaining);
            return remaining;
        }

        /// <summary>
        /// Removes settled groups; drainer only.
        /// </summary>
        internal void Prune() => _pending.RemoveAll(x => x.Remaining == 0);

        /// <summary>
        /// Drops every queued hint without a delete; drainer only, or after the last handle.
        /// </summary>
        internal int DropAll()
        {
            var dropped = 0;
            foreach (var group in Pending())
            {
                dropped += Drop(group);
            }
            _pending.Clear();
            Publish();
            return dropped;
        }

        /// <summary>
        /// Drops every queued hint once the last handle settled, counted for each group's producer.
        /// </summary>
        internal int DropReleased()
        {
            var dropped = 0;
            foreach (var group in Pending())
            {
                var count = Drop(group);
                group.Counters?.AddDropped(count);
                dropped += count;
            }
            _pending.Clear();
            // Holds nothing back from the pressure any more
            Publish();
            return dropped;
        }

        // A pending group's facts the due check reads
        private readonly record struct Pended(long Version, int Hints, DateTimeOffset QueuedAt, DateTimeOffset ReadyAt);
    }
}
