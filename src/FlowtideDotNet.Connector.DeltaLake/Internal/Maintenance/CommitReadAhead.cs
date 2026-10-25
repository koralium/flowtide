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
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// A listed log object opened for its turn; the reader built on the stream owns it, and disposing this releases the admission.
    /// </summary>
    internal readonly struct OpenedCommit : IDisposable
    {
        public OpenedCommit(Stream? stream, IDisposable? admission, bool stopped)
        {
            Stream = stream;
            Admission = admission;
            Stopped = stopped;
        }

        // Null when the object is gone
        public Stream? Stream { get; }

        public IDisposable? Admission { get; }

        // Issuance stopped before the object was opened
        public bool Stopped { get; }

        public void Dispose() => Admission?.Dispose();
    }

    /// <summary>
    /// Opens listed log objects in order up to a window ahead of a consumer that takes each in turn.
    /// </summary>
    internal sealed class CommitReadAhead : IAsyncDisposable
    {
        public const int MinDepth = 20;

        public const int MaxDepth = 512;

        // Listed bytes of the objects opened and not yet taken
        public const long WindowBytes = 16L << 20;

        private readonly IFileStorage _storage;
        private readonly Func<long, IOEntry?> _entryAt;
        private readonly long _count;
        private readonly TransferAllowance? _transfer;
        private readonly Func<bool> _mayIssue;
        private readonly CancellationTokenSource _cancel;
        private readonly Slot[] _slots;
        // The metered storage's request bucket, null when the storage is not metered
        private readonly FifoTokenBucket? _requests;
        // Tokens ahead opens leave in the bucket for other jobs, a tenth of its burst
        private readonly long _reserve;
        // Consumer state
        private long _issued;
        private long _taken;
        private long _bytes;
        private bool _halted;

        public CommitReadAhead(IFileStorage storage, Func<long, IOEntry?> entryAt, long count, int depth, TransferAllowance? transfer, Func<bool> mayIssue, CancellationToken cancellationToken)
        {
            _storage = storage;
            _entryAt = entryAt;
            _count = count;
            _transfer = transfer;
            _mayIssue = mayIssue;
            _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _requests = MeteredFileStorage.Of(storage)?.Buckets.Requests;
            _reserve = _requests == null ? 0 : _requests.Burst / 10;
            _slots = new Slot[Math.Clamp(Math.Min(depth, count), 1, MaxDepth)];
            for (int i = 0; i < _slots.Length; i++)
            {
                _slots[i] = new Slot(this);
            }
        }

        /// <summary>
        /// Objects opened ahead on stores whose clients take concurrent reads, a tenth of a second of the request rate; one elsewhere.
        /// </summary>
        public static int Depth(StoreDescriptor store, long requestsPerSecond)
        {
            // The pinned S3 signer, in-memory streams and unqualified stores read one object at a time
            if (store.Kind is not (StoreKind.LocalDisk or StoreKind.Azure))
            {
                return 1;
            }
            return (int)Math.Clamp((requestsPerSecond + 9) / 10, MinDepth, MaxDepth);
        }

        // Opened or opening and not yet taken
        public int Outstanding => (int)(_issued - _taken);

        /// <summary>
        /// The next object in order; the failure of its open is thrown here and nowhere else.
        /// </summary>
        public ValueTask<OpenedCommit> NextAsync()
        {
            if (_taken >= _count)
            {
                throw new InvalidOperationException($"All {_count} objects were taken");
            }
            // The consumer checked its own stop before asking
            if (_issued == _taken)
            {
                Issue(_entryAt(_issued) ?? throw new InvalidOperationException($"Object {_issued} of the run is not listed"), ahead: false);
            }
            TopUp();
            var slot = _slots[_taken % _slots.Length];
            _taken++;
            _bytes -= slot.Bytes;
            return slot.Take();
        }

        private void TopUp()
        {
            while (!_halted && _issued < _count && _issued - _taken < _slots.Length)
            {
                // An unlisted or large object waits until it is the head
                var entry = _entryAt(_issued);
                if (entry == null || _bytes + Size(entry) > WindowBytes)
                {
                    return;
                }
                // Ahead opens never wait in the request bucket or behind an open still waiting for its admission
                if (_slots[(_issued - 1) % _slots.Length].Admitting || (_requests != null && !_requests.HasSpare(_reserve)))
                {
                    return;
                }
                if (_cancel.IsCancellationRequested || !_mayIssue())
                {
                    _halted = true;
                    return;
                }
                Issue(entry, ahead: true);
            }
        }

        private void Issue(IOEntry entry, bool ahead)
        {
            var size = Size(entry);
            var slot = _slots[_issued % _slots.Length];
            _issued++;
            _bytes += size;
            slot.Start(entry.Path, size, ahead);
        }

        // As the listed size is admitted
        private static long Size(IOEntry entry) => Math.Max(0, entry.Size ?? 0);

        public async ValueTask DisposeAsync()
        {
            if (_issued > _taken)
            {
                _cancel.Cancel();
            }
            while (_taken < _issued)
            {
                var slot = _slots[_taken % _slots.Length];
                _taken++;
                try
                {
                    var opened = await slot.Take();
                    try
                    {
                        if (opened.Stream != null)
                        {
                            await opened.Stream.DisposeAsync();
                        }
                    }
                    finally
                    {
                        opened.Dispose();
                    }
                }
                catch (Exception)
                {
                    // Never its turn
                }
            }
            _cancel.Dispose();
        }

        // One position of the window, reused for every object at it
        private sealed class Slot : IValueTaskSource<OpenedCommit>
        {
            private readonly CommitReadAhead _owner;
            private readonly Action _admitted;
            private readonly Action _opened;
            private ManualResetValueTaskSourceCore<OpenedCommit> _core;
            private ConfiguredTaskAwaitable<IDisposable>.ConfiguredTaskAwaiter _admission;
            private ConfiguredTaskAwaitable<Stream?>.ConfiguredTaskAwaiter _open;
            private IDisposable? _charge;
            private IOPath? _path;
            private bool _ahead;
            private volatile bool _admitting;

            public Slot(CommitReadAhead owner)
            {
                _owner = owner;
                _admitted = OnAdmitted;
                _opened = OnOpened;
                _core.RunContinuationsAsynchronously = true;
            }

            // The listed bytes of its current object
            public long Bytes { get; private set; }

            // Its object waits for its transfer admission
            public bool Admitting => _admitting;

            public void Start(IOPath path, long bytes, bool ahead)
            {
                _core.Reset();
                _path = path;
                _ahead = ahead;
                Bytes = bytes;
                if (_owner._transfer is not { } transfer)
                {
                    Open();
                    return;
                }
                _admission = transfer.AdmitAsync(bytes, _owner._cancel.Token).ConfigureAwait(false).GetAwaiter();
                if (_admission.IsCompleted)
                {
                    OnAdmitted();
                }
                else
                {
                    _admitting = true;
                    _admission.UnsafeOnCompleted(_admitted);
                }
            }

            private void OnAdmitted()
            {
                _admitting = false;
                IDisposable charge;
                try
                {
                    charge = _admission.GetResult();
                }
                catch (Exception e)
                {
                    _admission = default;
                    _core.SetException(e);
                    return;
                }
                _admission = default;
                // A stop seen while it waited opens nothing more ahead
                if (_ahead && (_owner._cancel.IsCancellationRequested || !_owner._mayIssue()))
                {
                    charge.Dispose();
                    _core.SetResult(new OpenedCommit(null, null, stopped: true));
                    return;
                }
                _charge = charge;
                Open();
            }

            private void Open()
            {
                try
                {
                    _open = _owner._storage.OpenRead(_path!, _owner._cancel.Token).ConfigureAwait(false).GetAwaiter();
                }
                catch (Exception e)
                {
                    Fail(e);
                    return;
                }
                if (_open.IsCompleted)
                {
                    OnOpened();
                }
                else
                {
                    _open.UnsafeOnCompleted(_opened);
                }
            }

            private void OnOpened()
            {
                Stream? stream;
                try
                {
                    stream = _open.GetResult();
                }
                catch (Exception e)
                {
                    _open = default;
                    Fail(e);
                    return;
                }
                _open = default;
                var charge = _charge;
                _charge = null;
                _core.SetResult(new OpenedCommit(stream, charge, stopped: false));
            }

            private void Fail(Exception e)
            {
                var charge = _charge;
                _charge = null;
                charge?.Dispose();
                _core.SetException(e);
            }

            // Awaited once, by the consumer or by the window's disposal
            public ValueTask<OpenedCommit> Take() => new ValueTask<OpenedCommit>(this, _core.Version);

            public OpenedCommit GetResult(short token) => _core.GetResult(token);

            public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

            public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);
        }
    }
}
