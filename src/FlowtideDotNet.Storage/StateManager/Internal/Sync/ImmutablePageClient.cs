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

using FlowtideDotNet.Storage.Persistence;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading.Channels;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal sealed class ImmutablePageClientOptions<V>
        where V : class, ICacheObject
    {
        public ImmutablePageClientOptions(Func<IStateSerializer<V>> serializerFactory)
        {
            SerializerFactory = serializerFactory;
        }

        internal void Validate()
        {
            if (WriteQueueCapacity < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(WriteQueueCapacity), "The write queue must hold at least one page.");
            }
            if (WindowSize < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(WindowSize), "A page window must hold at least one page.");
            }
        }

        /// <summary>
        /// Creates a serializer, one is used by each load in flight.
        /// </summary>
        public Func<IStateSerializer<V>> SerializerFactory { get; }

        /// <summary>
        /// Pages that can wait to be written.
        /// </summary>
        public int WriteQueueCapacity { get; set; } = 16;

        /// <summary>
        /// Pages one reader can hold at a time.
        /// </summary>
        public int WindowSize { get; set; } = 4;
    }

    /// <summary>
    /// Pages that never change once published, for many readers.
    /// </summary>
    internal sealed class ImmutablePageClient<V> : StateClient, ICacheEvictHandler
        where V : class, ICacheObject
    {
        private readonly struct QueuedWrite
        {
            public QueuedWrite(long pageId, V page)
            {
                PageId = pageId;
                Page = page;
                Drained = null;
                Commit = false;
            }

            public QueuedWrite(TaskCompletionSource drained, bool commit)
            {
                PageId = 0;
                Page = null;
                Drained = drained;
                Commit = commit;
            }

            public long PageId { get; }

            public V? Page { get; }

            // Set on a marker, completed when the writer reaches it
            public TaskCompletionSource? Drained { get; }

            public bool Commit { get; }
        }

        // One load of a page, later readers join it
        private sealed class Load
        {
            public Load(int generation)
            {
                Generation = generation;
                Completion = new TaskCompletionSource<V?>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            public int Generation { get; }

            public TaskCompletionSource<V?> Completion { get; }
        }

        private readonly S3FifoTableSync _table;
        private readonly Func<long> _newPageId;
        private readonly long _metadataId;
        private readonly IPersistentStorageSession _session;
        private readonly Func<long, IStateSerializer<V>, ValueTask<V>> _readPage;
        private readonly Func<IStateSerializer<V>> _serializerFactory;
        private readonly int _windowSize;
        private readonly int _writeQueueCapacity;
        private readonly ConcurrentDictionary<long, Load> _loads = new ConcurrentDictionary<long, Load>();
        private readonly Channel<QueuedWrite> _writes;
        private readonly SemaphoreSlim _writeSlots;
        private readonly IStateSerializer<V> _writeSerializer;
        private readonly Task _writer;
        // Guards the generation and the closed flag, held for an admission
        private readonly object _admitLock = new object();
        private readonly object _poolLock = new object();
        private readonly Stack<IStateSerializer<V>> _pool = new Stack<IStateSerializer<V>>();
        private readonly object _windowLock = new object();
        private readonly List<PageWindow<V>> _windows = new List<PageWindow<V>>();
        private Exception? _writeFault;
        private Task? _commitTask;
        private Task? _disposal;
        private int _generation;
        private bool _closed;
        private bool _poolClosed;
        private int _disposed;
        private long _hitsOfClosedWindows;
        private int _queuedPages;
        private int _loadsInFlight;
        private long _queueFullWaits;
        private bool _dropWrites;
        private bool _droppedPages;
        private bool _stopCommits;

        public ImmutablePageClient(
            S3FifoTableSync table,
            Func<long> newPageId,
            string name,
            long metadataId,
            IPersistentStorageSession session,
            Func<long, IStateSerializer<V>, ValueTask<V>> readPage,
            ImmutablePageClientOptions<V> options,
            Meter meter)
        {
            _table = table;
            _newPageId = newPageId;
            _metadataId = metadataId;
            _session = session;
            _readPage = readPage;
            _serializerFactory = options.SerializerFactory;
            _windowSize = options.WindowSize;
            _writeQueueCapacity = options.WriteQueueCapacity;
            _writeSlots = new SemaphoreSlim(_writeQueueCapacity, _writeQueueCapacity);
            _writes = Channel.CreateUnbounded<QueuedWrite>(new UnboundedChannelOptions() { SingleReader = true });
            _writeSerializer = _serializerFactory();
            // The queue's pages count against the cache's bound from the start
            if (!table.TryReserveHeldPages(_writeQueueCapacity))
            {
                _writeSerializer.Dispose();
                throw new InvalidOperationException("The write queue would hold more pages than the cache allows.");
            }

            var tagList = new TagList();
            tagList.Add("state_client", name);
            meter.CreateObservableGauge("flowtide_page_client_held_pages", () => new Measurement<int>(HeldPages, tagList));
            meter.CreateObservableGauge("flowtide_page_client_loads_in_flight", () => new Measurement<int>(LoadsInFlight, tagList));
            meter.CreateObservableCounter("flowtide_page_client_queue_full_waits", () => new Measurement<long>(QueueFullWaits, tagList));
            // Hits are counted per reader and summed for the table
            table.RegisterExternalHitCounter(CountHits);
            _writer = Task.Run(WriteLoop);
        }

        public override long MetadataId => _metadataId;

        public int WindowSize => _windowSize;

        /// <summary>
        /// Pages held by readers and by the write queue.
        /// </summary>
        public int HeldPages
        {
            get
            {
                int held = Volatile.Read(ref _queuedPages);
                lock (_windowLock)
                {
                    foreach (var window in _windows)
                    {
                        held += window.Count;
                    }
                }
                return held;
            }
        }

        private long CountHits()
        {
            lock (_windowLock)
            {
                long hits = _hitsOfClosedWindows;
                foreach (var window in _windows)
                {
                    hits += window.Hits;
                }
                return hits;
            }
        }

        public int LoadsInFlight => Volatile.Read(ref _loadsInFlight);

        /// <summary>
        /// Test hook, runs when a reader joins another reader's load.
        /// </summary>
        internal Action? LoadJoinedHookForTests { get; set; }

        /// <summary>
        /// True while a restore or a dispose keeps pages out.
        /// </summary>
        internal bool AdmissionClosed
        {
            get
            {
                lock (_admitLock)
                {
                    return _closed;
                }
            }
        }

        public long QueueFullWaits => Interlocked.Read(ref _queueFullWaits);

        public long NewPageId()
        {
            return _newPageId();
        }

        /// <summary>
        /// Registers a reader, it holds a few pages.
        /// </summary>
        public PageWindow<V> CreateWindow()
        {
            lock (_windowLock)
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                if (!_table.TryReserveHeldPages(_windowSize))
                {
                    throw new InvalidOperationException("Too many readers for the cache, they could hold more pages than allowed.");
                }
                var window = new PageWindow<V>(this, _windowSize);
                _windows.Add(window);
                return window;
            }
        }

        // Also after a dispose, a window may still hold pages
        internal void RemoveWindow(PageWindow<V> window)
        {
            lock (_windowLock)
            {
                if (_windows.Remove(window))
                {
                    _hitsOfClosedWindows += window.Hits;
                    _table.ReleaseHeldPages(_windowSize);
                }
            }
        }

        // Called with the admit lock held
        private void ThrowIfClosed()
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_closed)
            {
                throw new InvalidOperationException("The page cache is being reset.");
            }
        }

        // Called with the admit lock held, before anything is added
        private void ThrowIfClosedOrReset(int generation)
        {
            ThrowIfClosed();
            if (_generation != generation)
            {
                throw new InvalidOperationException("The page cache was reset during the call.");
            }
        }

        private void ThrowIfReset(int generation)
        {
            if (Volatile.Read(ref _generation) != generation)
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                throw new InvalidOperationException("The page cache was reset during the read.");
            }
        }

        /// <summary>
        /// Caches a new page and queues it to be written.
        /// The client takes over the page, unless this throws.
        /// </summary>
        public ValueTask PublishAsync(long pageId, V page)
        {
            // Read before any wait, a restore in between refuses the page
            int generation = Volatile.Read(ref _generation);
            // Slot first, so a waiting page is never cached
            if (_writeSlots.Wait(0))
            {
                Publish(pageId, page, generation);
                return ValueTask.CompletedTask;
            }
            return PublishAsync_Slow(pageId, page, generation);
        }

        private async ValueTask PublishAsync_Slow(long pageId, V page, int generation)
        {
            Interlocked.Increment(ref _queueFullWaits);
            await _writeSlots.WaitAsync().ConfigureAwait(false);
            Publish(pageId, page, generation);
        }

        // Owns a write slot on entry, the writer gives it back
        private void Publish(long pageId, V page, int generation)
        {
            bool rented = false;
            try
            {
                var fault = Volatile.Read(ref _writeFault);
                if (fault != null)
                {
                    throw new InvalidOperationException("An earlier page could not be written.", fault);
                }
                if (!page.TryRent())
                {
                    throw new InvalidOperationException("The page is already disposed.");
                }
                rented = true;
                lock (_admitLock)
                {
                    ThrowIfClosedOrReset(generation);
                    // The id is new, an entry already there is stale
                    if (_table.TryPeekEntry(pageId, out _))
                    {
                        _table.Delete(pageId);
                    }
                    _table.Add(pageId, page, this);
                    // Queued under the lock, never behind a restore's marker
                    Interlocked.Increment(ref _queuedPages);
                    bool queued = _writes.Writer.TryWrite(new QueuedWrite(pageId, page));
                    Debug.Assert(queued, "The queue is completed only after the cache is closed");
                }
            }
            catch
            {
                if (rented)
                {
                    page.Return();
                }
                _writeSlots.Release();
                throw;
            }
        }

        /// <summary>
        /// Rents a page that is in the cache, without waiting.
        /// </summary>
        public bool TryGet(long pageId, PageWindow<V> window, [NotNullWhen(true)] out V? page)
        {
            window.ThrowIfFull();
            // No counter that readers share is written on this path
            if (_table.TryPeekEntry(pageId, out var entry) && entry.TryRentValue())
            {
                page = (V)entry.Value;
                window.Hold(page);
                window.Hit();
                return true;
            }
            page = null;
            return false;
        }

        /// <summary>
        /// Rents a page, loaded from storage if not cached.
        /// </summary>
        public ValueTask<V> GetAsync(long pageId, PageWindow<V> window)
        {
            if (TryGet(pageId, window, out var page))
            {
                return ValueTask.FromResult(page);
            }
            return GetAsync_Slow(pageId, window);
        }

        private async ValueTask<V> GetAsync_Slow(long pageId, PageWindow<V> window)
        {
            int generation;
            lock (_admitLock)
            {
                ThrowIfClosed();
                generation = _generation;
            }
            while (true)
            {
                V? page = null;
                Load? mine = null;
                if (_loads.TryGetValue(pageId, out var load))
                {
                    if (load.Generation == generation)
                    {
                        // Someone else loads it, rent what they loaded
                        LoadJoinedHookForTests?.Invoke();
                        page = await load.Completion.Task.ConfigureAwait(false);
                        if (page != null && !page.TryRent())
                        {
                            page = null;
                        }
                    }
                    else
                    {
                        ThrowIfReset(generation);
                        // A load from before a reset adds nothing, take its place
                        mine = new Load(generation);
                        if (!_loads.TryUpdate(pageId, mine, load))
                        {
                            continue;
                        }
                    }
                }
                else
                {
                    mine = new Load(generation);
                    if (!_loads.TryAdd(pageId, mine))
                    {
                        continue;
                    }
                }
                if (mine != null)
                {
                    page = await LoadAsync(pageId, mine).ConfigureAwait(false);
                }
                if (page != null)
                {
                    window.Hold(page);
                    return page;
                }
                ThrowIfReset(generation);
                if (TryGet(pageId, window, out var cached))
                {
                    return cached;
                }
            }
        }

        // The caller's rented page, or null if none is cached
        private async ValueTask<V?> LoadAsync(long pageId, Load load)
        {
            V? result = null;
            V? page = null;
            int rents = 0;
            Exception? failure = null;
            IStateSerializer<V>? serializer = null;
            Interlocked.Increment(ref _loadsInFlight);
            try
            {
                // A load that just ended may have cached it
                if (_table.TryPeekEntry(pageId, out var resident) && resident.TryRentValue())
                {
                    result = (V)resident.Value;
                    return result;
                }
                serializer = RentSerializer();
                // Read and decode run outside every lock
                page = await _readPage(pageId, serializer).ConfigureAwait(false);
                rents = 1;
                if (!page.TryRent())
                {
                    throw new InvalidOperationException("The loaded page is already disposed.");
                }
                rents = 2;
                while (true)
                {
                    S3FifoCacheEntry entry;
                    bool added;
                    lock (_admitLock)
                    {
                        // A reset cannot come between this check and the add
                        ThrowIfClosedOrReset(load.Generation);
                        added = _table.AddIfAbsent(pageId, page, this, out entry);
                    }
                    if (added)
                    {
                        // The cache has the first rent, the caller the second
                        rents = 0;
                        result = page;
                        return result;
                    }
                    if (entry.TryRentValue())
                    {
                        // The key holds another object, use that one
                        result = (V)entry.Value;
                        return result;
                    }
                    // That entry is on its way out, try again
                    Thread.Yield();
                }
            }
            catch (Exception e)
            {
                failure = e;
                throw;
            }
            finally
            {
                // Removed first, so no reader joins a finished load
                _loads.TryRemove(new KeyValuePair<long, Load>(pageId, load));
                if (failure != null)
                {
                    load.Completion.SetException(failure);
                    // Observed here, there may be no other reader waiting
                    _ = load.Completion.Task.Exception;
                }
                else
                {
                    load.Completion.SetResult(result);
                }
                Interlocked.Decrement(ref _loadsInFlight);
                // Last, a cleanup that throws leaves no reader waiting
                try
                {
                    for (int i = 0; i < rents; i++)
                    {
                        page!.Return();
                    }
                }
                finally
                {
                    if (serializer != null)
                    {
                        ReturnSerializer(serializer);
                    }
                }
            }
        }

        private IStateSerializer<V> RentSerializer()
        {
            lock (_poolLock)
            {
                if (_pool.Count > 0)
                {
                    return _pool.Pop();
                }
            }
            return _serializerFactory();
        }

        private void ReturnSerializer(IStateSerializer<V> serializer)
        {
            lock (_poolLock)
            {
                if (!_poolClosed)
                {
                    _pool.Push(serializer);
                    return;
                }
            }
            DisposeQuietly(serializer);
        }

        /// <summary>
        /// Clean pages are dropped, a queued page is rented and stays.
        /// </summary>
        public Task<int> Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
        {
            return Task.FromResult(valuesToEvict.Count);
        }

        private async Task WriteLoop()
        {
            await foreach (var write in _writes.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                if (write.Drained != null)
                {
                    await ReachMarker(write.Drained, write.Commit).ConfigureAwait(false);
                    continue;
                }
                var page = write.Page!;
                try
                {
                    if (Volatile.Read(ref _dropWrites))
                    {
                        // Remembered, no commit may go on without this page
                        Volatile.Write(ref _droppedPages, true);
                    }
                    else if (Volatile.Read(ref _writeFault) == null)
                    {
                        await _session.Write(write.PageId, new SerializableObject(page, _writeSerializer)).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref _writeFault, e, null);
                }
                // Rent first, a freed slot must not add held pages
                try
                {
                    page.Return();
                }
                catch (Exception e)
                {
                    Interlocked.CompareExchange(ref _writeFault, e, null);
                }
                Interlocked.Decrement(ref _queuedPages);
                _writeSlots.Release();
            }
        }

        // Only the writer uses the session, also for a commit
        private async Task ReachMarker(TaskCompletionSource drained, bool commit)
        {
            try
            {
                var fault = Volatile.Read(ref _writeFault);
                if (fault != null)
                {
                    drained.TrySetException(fault);
                    return;
                }
                if (commit)
                {
                    if (Volatile.Read(ref _dropWrites) || Volatile.Read(ref _droppedPages) || Volatile.Read(ref _stopCommits))
                    {
                        // Pages were dropped or a stop was asked, so no commit
                        drained.TrySetCanceled();
                        return;
                    }
                    await _session.Commit().ConfigureAwait(false);
                }
                drained.TrySetResult();
            }
            catch (Exception e)
            {
                Interlocked.CompareExchange(ref _writeFault, e, null);
                drained.TrySetException(e);
            }
        }

        private Task QueueMarker(bool commit)
        {
            var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_writes.Writer.TryWrite(new QueuedWrite(drained, commit)))
            {
                drained.TrySetException(new ObjectDisposedException(nameof(ImmutablePageClient<V>)));
            }
            return drained.Task;
        }

        /// <summary>
        /// Completes when every page published so far is written.
        /// </summary>
        public Task DrainAsync()
        {
            return QueueMarker(false);
        }

        /// <summary>
        /// Writes what is queued and commits, called at a checkpoint.
        /// </summary>
        public Task CommitAsync()
        {
            var commit = QueueMarker(true);
            Volatile.Write(ref _commitTask, commit);
            return commit;
        }

        internal override bool HasCommitInFlight => Volatile.Read(ref _commitTask) is { IsCompleted: false };

        internal override bool HasCommitFault => Volatile.Read(ref _writeFault) != null;

        internal override Exception? CommitFault => Volatile.Read(ref _writeFault);

        internal override void ClearCommitFault()
        {
            Volatile.Write(ref _writeFault, null);
        }

        internal override Task WaitForCommitAsync()
        {
            return Volatile.Read(ref _commitTask) ?? Task.CompletedTask;
        }

        internal override void RequestStopCommits()
        {
            // Only commits give up, pages are still written for readers
            Volatile.Write(ref _stopCommits, true);
        }

        internal override void StopCacheUse()
        {
            lock (_admitLock)
            {
                // The cache is disposed next, nothing may be added to it
                _closed = true;
                _generation++;
            }
        }

        /// <summary>
        /// Before a restore: closes the cache and stops the writer.
        /// </summary>
        internal override async Task PauseCommitsAsync(TimeSpan walkTimeout)
        {
            lock (_admitLock)
            {
                // From here the old state can add no page
                _closed = true;
                _generation++;
            }
            Volatile.Write(ref _dropWrites, true);
            // The marker is behind every write and commit queued so far
            if (!await WaitQuietlyAsync(DrainAsync(), walkTimeout).ConfigureAwait(false))
            {
                throw new TimeoutException("The immutable page writer did not stop in time for the restore.");
            }
            Volatile.Write(ref _commitTask, null);
            // A load that runs on is harmless, it can add nothing
            var loads = new List<Task>();
            foreach (var load in _loads.Values)
            {
                loads.Add(load.Completion.Task);
            }
            await WaitQuietlyAsync(Task.WhenAll(loads), walkTimeout).ConfigureAwait(false);
        }

        // False on a timeout, the task's failure is ignored
        private static async Task<bool> WaitQuietlyAsync(Task task, TimeSpan timeout)
        {
            using var stop = new CancellationTokenSource();
            var done = await Task.WhenAny(task, Task.Delay(timeout, stop.Token)).ConfigureAwait(false);
            stop.Cancel();
            if (task.IsFaulted)
            {
                _ = task.Exception;
            }
            return done == task;
        }

        internal override void ResumeCommits()
        {
            lock (_admitLock)
            {
                // Writes go on before a publish can queue one
                Volatile.Write(ref _dropWrites, false);
                _closed = false;
            }
        }

        public override ValueTask Reset(bool clearMetadata)
        {
            lock (_admitLock)
            {
                _generation++;
            }
            // The dropped pages belonged to the rolled back state
            Volatile.Write(ref _droppedPages, false);
            Volatile.Write(ref _stopCommits, false);
            return ValueTask.CompletedTask;
        }

        internal override Task DisposalTask => Volatile.Read(ref _disposal) ?? Task.CompletedTask;

        public override void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }
            lock (_admitLock)
            {
                // Closed for good, a late load or publish adds nothing
                _closed = true;
                _generation++;
            }
            Volatile.Write(ref _dropWrites, true);
            _writes.Writer.TryComplete();
            // The session goes when its writer has ended
            var disposal = _writer.ContinueWith(static (writer, state) => ((ImmutablePageClient<V>)state!).DisposeAfterWriter(writer), this, TaskScheduler.Default);
            Volatile.Write(ref _disposal, disposal);
        }

        private void DisposeAfterWriter(Task writer)
        {
            // Nobody awaits the writer, its fault ends here
            _ = writer.Exception;
            DisposeResources();
            // The writer has returned every queued page by now
            _table.ReleaseHeldPages(_writeQueueCapacity);
        }

        private void DisposeResources()
        {
            List<IStateSerializer<V>> pooled;
            lock (_poolLock)
            {
                _poolClosed = true;
                pooled = new List<IStateSerializer<V>>(_pool);
                _pool.Clear();
            }
            // One that throws must not keep the others open
            foreach (var serializer in pooled)
            {
                DisposeQuietly(serializer);
            }
            DisposeQuietly(_writeSerializer);
            DisposeQuietly(_session);
        }

        private static void DisposeQuietly(IDisposable resource)
        {
            try
            {
                resource.Dispose();
            }
            catch
            {
                // A teardown fallback, there is nobody left to tell.
            }
        }
    }
}
