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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using Stowage.Impl;
using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;
using Xunit.Sdk;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // A connector-owned hook wrapper that records which commits were opened, read and disposed
    internal class TrackedHookFileStorage : HookFileStorage, IFileStorage, IConnectorOwnedStorage
    {
        private static readonly Regex s_commit = new Regex(@"^\d{20}\.json$", RegexOptions.Compiled);
        private readonly ConcurrentQueue<long> _opened = new ConcurrentQueue<long>();
        private readonly ConcurrentDictionary<long, byte> _read = new ConcurrentDictionary<long, byte>();
        private int _entered;
        private int _returned;
        private int _disposed;
        private int _maxOutstanding;

        public TrackedHookFileStorage(IFileStorage inner) : base(inner)
        {
        }

        // Keeps the local disk interface visible through the wrapper
        public static TrackedHookFileStorage Over(IFileStorage inner)
        {
            return inner is ILocalDiskFileStorage ? new LocalDiskTrackedHookFileStorage(inner) : new TrackedHookFileStorage(inner);
        }

        // Commit versions in the order their opens entered, also those that threw or found nothing
        public IReadOnlyList<long> Opened => _opened.ToArray();

        // Commit versions whose stream was read
        public IReadOnlyList<long> Read => _read.Keys.Order().ToList();

        public int Returned => Volatile.Read(ref _returned);

        public int Disposed => Volatile.Read(ref _disposed);

        // The most commit opens entered and not yet disposed at once
        public int MaxOutstanding => Volatile.Read(ref _maxOutstanding);

        async Task<Stream?> IFileStorage.OpenRead(IOPath path, CancellationToken cancellationToken)
        {
            if (!s_commit.IsMatch(path.Name))
            {
                return await base.OpenRead(path, cancellationToken);
            }
            var version = long.Parse(path.Name.AsSpan(0, 20));
            _opened.Enqueue(version);
            var outstanding = Interlocked.Increment(ref _entered) - Volatile.Read(ref _disposed);
            int max;
            while (outstanding > (max = Volatile.Read(ref _maxOutstanding)) && Interlocked.CompareExchange(ref _maxOutstanding, outstanding, max) != max)
            {
            }
            var stream = await base.OpenRead(path, cancellationToken);
            if (stream is not MemoryStream memory)
            {
                return stream;
            }
            var tracked = new TrackedStream(memory.ToArray(), this, version);
            await memory.DisposeAsync();
            Interlocked.Increment(ref _returned);
            return tracked;
        }

        private sealed class TrackedStream : MemoryStream
        {
            private readonly TrackedHookFileStorage _owner;
            private readonly long _version;
            private int _disposed;

            public TrackedStream(byte[] bytes, TrackedHookFileStorage owner, long version) : base(bytes, false)
            {
                _owner = owner;
                _version = version;
            }

            private void Reading() => _owner._read.TryAdd(_version, 0);

            public override int Read(byte[] buffer, int offset, int count)
            {
                Reading();
                return base.Read(buffer, offset, count);
            }

            public override int Read(Span<byte> buffer)
            {
                Reading();
                return base.Read(buffer);
            }

            public override int ReadByte()
            {
                Reading();
                return base.ReadByte();
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                Reading();
                return base.ReadAsync(buffer, offset, count, cancellationToken);
            }

            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                Reading();
                return base.ReadAsync(buffer, cancellationToken);
            }

            protected override void Dispose(bool disposing)
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    Interlocked.Increment(ref _owner._disposed);
                }
                base.Dispose(disposing);
            }
        }
    }

    // The tracked wrapper over local disk, which keeps native paths reachable
    internal sealed class LocalDiskTrackedHookFileStorage : TrackedHookFileStorage, ILocalDiskFileStorage
    {
        public LocalDiskTrackedHookFileStorage(IFileStorage inner) : base(inner)
        {
        }

        public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);
    }

    // Parks gated opens and releases them in rounds, newest first
    internal sealed class OpenRounds
    {
        private readonly ConcurrentQueue<TaskCompletionSource> _parked = new ConcurrentQueue<TaskCompletionSource>();

        public int Parked => _parked.Count;

        public Func<string, IOPath, Task> Hook(Func<IOPath, bool> gated) => (verb, path) =>
        {
            if (verb != "OpenRead" || !gated(path))
            {
                return Task.CompletedTask;
            }
            var open = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _parked.Enqueue(open);
            return open.Task;
        };

        // Each round waits until the depth, or what is left of the total, is parked; only for work that succeeds, a failing job's dispose waits for parked opens
        public async Task<int> DriveAsync(Task work, int depth, int total)
        {
            var rounds = 0;
            var released = 0;
            while (true)
            {
                var expected = Math.Min(depth, total - released);
                try
                {
                    await MaintenanceBucketsTests.Until(() => work.IsCompleted || (expected > 0 && _parked.Count == expected));
                }
                catch (XunitException)
                {
                    Assert.Fail($"Round {rounds + 1} waited for {expected} parked opens and saw {_parked.Count}");
                }
                if (work.IsCompleted)
                {
                    return rounds;
                }
                var batch = new List<TaskCompletionSource>();
                while (_parked.TryDequeue(out var open))
                {
                    batch.Add(open);
                }
                Assert.Equal(expected, batch.Count);
                released += batch.Count;
                rounds++;
                for (int i = batch.Count - 1; i >= 0; i--)
                {
                    batch[i].SetResult();
                }
            }
        }

        // Releases whatever is parked, newest first, until the work completes
        public async Task PumpAsync(Task work)
        {
            while (true)
            {
                await MaintenanceBucketsTests.Until(() => work.IsCompleted || !_parked.IsEmpty);
                var batch = new List<TaskCompletionSource>();
                while (_parked.TryDequeue(out var open))
                {
                    batch.Add(open);
                }
                for (int i = batch.Count - 1; i >= 0; i--)
                {
                    batch[i].SetResult();
                }
                if (work.IsCompleted && _parked.IsEmpty)
                {
                    return;
                }
            }
        }
    }

    // Opens whose results a test completes by path; supports nothing else
    internal sealed class ScriptedOpens : IFileStorage
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource<Stream?>> _opens = new ConcurrentDictionary<string, TaskCompletionSource<Stream?>>(StringComparer.Ordinal);
        private readonly ConcurrentQueue<string> _calls = new ConcurrentQueue<string>();
        private readonly ConcurrentDictionary<string, CancellationToken> _tokens = new ConcurrentDictionary<string, CancellationToken>(StringComparer.Ordinal);

        // A log object of the run at the index
        public static IOEntry Entry(long index, long size = 10) => new IOEntry(PathOf(index)) { Size = size };

        public static string PathOf(long index) => $"/t/_delta_log/{index:D20}.json";

        // The open of the index, created on demand
        public TaskCompletionSource<Stream?> Open(long index) => _opens.GetOrAdd(PathOf(index), _ => new TaskCompletionSource<Stream?>(TaskCreationOptions.RunContinuationsAsynchronously));

        // Indices in the order their opens were called
        public IReadOnlyList<long> Calls => _calls.Select(x => long.Parse(x.AsSpan(x.Length - 25, 20))).ToList();

        public CancellationToken TokenOf(long index) => _tokens[PathOf(index)];

        public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
        {
            _calls.Enqueue(path.Full);
            _tokens[path.Full] = cancellationToken;
            return _opens.GetOrAdd(path.Full, _ => new TaskCompletionSource<Stream?>(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }

        public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task Rm(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public void Dispose()
        {
        }
    }
}
