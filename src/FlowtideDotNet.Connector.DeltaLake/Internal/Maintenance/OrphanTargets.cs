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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using System.Runtime.CompilerServices;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Writes an orphan run's targets in order to a job scratch file, which Finish moves out of the job.
    /// </summary>
    internal sealed class OrphanTargetsWriter : IDisposable
    {
        private readonly JobScratch _scratch;
        private readonly JobMemory _memory;
        private readonly RunWriter _writer;
        private byte[] _key = Array.Empty<byte>();
        private byte[] _entry = Array.Empty<byte>();
        // Set once the file left the job or was deleted
        private bool _gone;

        public OrphanTargetsWriter(JobScratch scratch, JobMemory memory)
        {
            _scratch = scratch;
            _memory = memory;
            _writer = new RunWriter(scratch, memory);
        }

        // The file in the job's folder
        public string Path => _writer.Path;

        public long Count { get; private set; }

        // UTF-8 bytes of the paths added
        public long PathBytes { get; private set; }

        // What a slice buffer charges for the frozen MD5s added
        public long Md5Bytes { get; private set; }

        /// <summary>
        /// Appends a target's exact deletion path and its frozen payload.
        /// </summary>
        public void Add(string path, ReadOnlySpan<byte> payload)
        {
            var keyLength = Encoding.UTF8.GetByteCount(path);
            Grow(ref _key, keyLength);
            Encoding.UTF8.GetBytes(path, _key);
            var length = ArenaEntry.EncodedLength(keyLength, payload.Length);
            Grow(ref _entry, length);
            ArenaEntry.Encode(_entry, _key.AsSpan(0, keyLength), 0, Count, payload);
            _writer.Write(_entry.AsSpan(0, length));
            Count++;
            PathBytes += keyLength;
            Md5Bytes += OrphanTargets.Md5BytesOf(payload);
        }

        /// <summary>
        /// Closes the file and moves it with its disk charge out of the job; a failed move leaves both in the job and throws.
        /// </summary>
        public OrphanTargets Finish()
        {
            _writer.Close();
            var owner = _scratch.Owner;
            var charge = _scratch.Release(Path);
            var target = System.IO.Path.Combine(owner.ProcessPath, "runs", Guid.NewGuid().ToString("N") + ".run");
            try
            {
                Directory.CreateDirectory(System.IO.Path.GetDirectoryName(target)!);
                File.Move(Path, target);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                _scratch.Adopt(Path, charge);
                throw;
            }
            _gone = true;
            FreeBuffers();
            return new OrphanTargets(owner, target, charge, Count, PathBytes, Md5Bytes);
        }

        private void Grow(ref byte[] buffer, int length)
        {
            if (buffer.Length >= length)
            {
                return;
            }
            var grown = _memory.Allocate(Math.Max(length, 256));
            if (buffer.Length > 0)
            {
                _memory.Free(buffer);
            }
            buffer = grown;
        }

        private void FreeBuffers()
        {
            _writer.Dispose();
            if (_key.Length > 0)
            {
                _memory.Free(_key);
                _key = Array.Empty<byte>();
            }
            if (_entry.Length > 0)
            {
                _memory.Free(_entry);
                _entry = Array.Empty<byte>();
            }
        }

        // Deletes a file that never left the job
        public void Dispose()
        {
            FreeBuffers();
            if (!_gone)
            {
                _gone = true;
                _scratch.Delete(Path);
            }
        }
    }

    /// <summary>
    /// A finished targets file in the process's scratch folder, charged in the disk ledger until it is disposed.
    /// </summary>
    internal sealed class OrphanTargets : IDisposable
    {
        private readonly ScratchDirectory _owner;
        private readonly long _charge;
        // Where the latest read started
        private ReadMark _mark = ReadMark.Start;
        private int _disposed;

        internal OrphanTargets(ScratchDirectory owner, string path, long charge, long count, long pathBytes, long md5Bytes)
        {
            _owner = owner;
            _charge = charge;
            Path = path;
            Count = count;
            PathBytes = pathBytes;
            Md5Bytes = md5Bytes;
        }

        public string Path { get; }

        public long Count { get; }

        // UTF-8 bytes of every target's path
        public long PathBytes { get; }

        // Path bytes from the latest read's start on
        public long PathBytesLeft => PathBytes - Volatile.Read(ref _mark).PathBytes;

        // What a slice buffer charges for every target's frozen MD5
        public long Md5Bytes { get; }

        // MD5 bytes from the latest read's start on
        public long Md5BytesLeft => Md5Bytes - Volatile.Read(ref _mark).Md5Bytes;

        // What a slice buffer charges for an entry's frozen MD5
        internal static long Md5BytesOf(ReadOnlySpan<byte> payload) => OrphanCandidatePayload.TryDecode(payload, out _, out _, out var md5) ? DeletionRun.Md5Bytes(md5) : 0;

        /// <summary>
        /// The targets from a position on, each slice reading the file again, from the latest read's start, under the memory the supplier returns.
        /// </summary>
        public IDeletionSource Source(DeletionExecutor executor, Func<JobMemory> memory) => new TargetSource(this, executor, memory);

        // Each entry's deletion path, size and modification time, in order
        internal List<(string Path, long? Size, DateTimeOffset? Modified)> ReadAllForTests()
        {
            var entries = new List<(string, long?, DateTimeOffset?)>();
            using var cursor = new RunCursor(Path, new JobMemory());
            while (cursor.MoveNext())
            {
                var (path, size, modified, _) = Read(cursor);
                entries.Add((path, size, modified));
            }
            return entries;
        }

        // The entry's path, and its size, time and MD5 when its payload decodes
        private static (string Path, long? Size, DateTimeOffset? Modified, string? Md5) Read(RunCursor cursor)
        {
            var entry = cursor.Current;
            var path = Encoding.UTF8.GetString(entry.Key);
            return OrphanCandidatePayload.TryDecode(entry.Record, out var size, out var modified, out var md5) ? (path, size, modified, md5) : (path, null, null, null);
        }

        /// <summary>
        /// Deletes the file, then releases its charge, once; a file that stays keeps its charge on the pending list.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _owner.DeleteAndRelease(Path, _charge);
            }
        }

        // An entry's position, the byte offset it starts at and the path and MD5 bytes before it
        private sealed record ReadMark(long Position, long Offset, long PathBytes, long Md5Bytes)
        {
            public static ReadMark Start { get; } = new ReadMark(0, 0, 0, 0);
        }

        private sealed class TargetSource : IDeletionSource
        {
            private readonly OrphanTargets _targets;
            private readonly DeletionExecutor _executor;
            private readonly Func<JobMemory> _memory;

            public TargetSource(OrphanTargets targets, DeletionExecutor executor, Func<JobMemory> memory)
            {
                _targets = targets;
                _executor = executor;
                _memory = memory;
            }

            public async IAsyncEnumerable<DeletionTarget> ReadFrom(long position, [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                await Task.CompletedTask;
                // From the latest read's start, or from the first entry for an earlier position
                var mark = Volatile.Read(ref _targets._mark);
                if (mark.Position > position)
                {
                    mark = ReadMark.Start;
                }
                using var cursor = new RunCursor(_targets.Path, _memory(), mark.Offset);
                var (index, offset, pathBytes, md5Bytes) = (mark.Position, mark.Offset, mark.PathBytes, mark.Md5Bytes);
                var more = cursor.MoveNext();
                while (more && index < position)
                {
                    offset += cursor.CurrentRaw.Length;
                    pathBytes += cursor.Current.Key.Length;
                    md5Bytes += Md5BytesOf(cursor.Current.Record);
                    index++;
                    more = cursor.MoveNext();
                }
                Volatile.Write(ref _targets._mark, new ReadMark(index, offset, pathBytes, md5Bytes));
                while (more)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return TargetOf(cursor);
                    more = cursor.MoveNext();
                }
            }

            private DeletionTarget TargetOf(RunCursor cursor)
            {
                var (path, size, modified, md5) = Read(cursor);
                return _executor.TargetOf(path) with { Size = size, Modified = modified, Md5 = md5 };
            }
        }
    }
}
