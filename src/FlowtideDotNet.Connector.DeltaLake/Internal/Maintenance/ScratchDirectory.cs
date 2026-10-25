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

using System.Collections.Concurrent;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The process's scratch folder, its lock, and its disk ledger; a file whose deletion failed keeps its charge on the pending list.
    /// </summary>
    internal sealed class ScratchDirectory
    {
        private readonly object _lock = new object();
        private readonly List<(string Path, long Charge)> _pending = new List<(string, long)>();
        private readonly ConcurrentQueue<(MaintenanceReason Reason, string Detail)> _notes = new ConcurrentQueue<(MaintenanceReason, string)>();
        private readonly FileStream _processLock;
        private long _sequence;

        // The budget passes its fact sequence; a directory of its own counts for itself
        public ScratchDirectory(string root, long diskCap = DiskLedger.DefaultCap, Func<long>? nextFactSequence = null, bool? locksEffectiveForTests = null)
        {
            Root = Path.GetFullPath(root);
            var id = Guid.NewGuid().ToString("N");
            ProcessPath = Path.Combine(Root, id);
            LockPath = ProcessPath + ".lock";
            Ledger = new DiskLedger(diskCap);
            Cache = new RootCache(this, nextFactSequence ?? (() => Interlocked.Increment(ref _sequence)));
            Ledger.Evict = Cache.EvictFor;

            // The lock comes before the folder and stays after the process, so a folder is only ever cleaned behind its own lock
            Directory.CreateDirectory(Root);
            _processLock = new FileStream(LockPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.None);
            try
            {
                Directory.CreateDirectory(ProcessPath);
                LocksEffective = locksEffectiveForTests ?? SecondOpenFails(LockPath);
                if (LocksEffective)
                {
                    CleanSiblings(id);
                }
                else
                {
                    _notes.Enqueue((MaintenanceReason.ScratchLocksIneffective, $"A second open of {LockPath} succeeded, so file locks do not protect scratch folders here and stale folders of other processes in {Root} are kept"));
                }
            }
            catch
            {
                // The next job creates a directory of its own
                _processLock.Dispose();
                throw;
            }
        }

        public string Root { get; }

        // This process's folder: jobs/ holds one folder per job, cache/ the cached roots
        public string ProcessPath { get; }

        // Held with FileShare.None while this process lives
        public string LockPath { get; }

        // False under DOTNET_SYSTEM_IO_DISABLEFILELOCKING or on file systems without locks, and cleanup is skipped
        public bool LocksEffective { get; }

        // The lock handle stays open for the directory's lifetime
        internal bool LockHeldForTests => _processLock.CanRead;

        // Test fixtures release the lock at teardown; the process-wide owner keeps it for the process's lifetime
        internal void ReleaseLockForTests() => _processLock.Dispose();

        /// <summary>
        /// What the folder's creation found worth a warning, taken once by the job that logs it.
        /// </summary>
        public IReadOnlyList<(MaintenanceReason Reason, string Detail)> TakeNotes()
        {
            var notes = new List<(MaintenanceReason, string)>();
            while (_notes.TryDequeue(out var note))
            {
                notes.Add(note);
            }
            return notes;
        }

        private static bool SecondOpenFails(string path)
        {
            try
            {
                using var second = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                return false;
            }
            catch (IOException)
            {
                return true;
            }
            catch (UnauthorizedAccessException)
            {
                return true;
            }
        }

        /// <summary>
        /// Deletes the folders of processes that are gone: only behind their own lock, only a plain folder named by its lock, never one that is or holds a link.
        /// </summary>
        private void CleanSiblings(string ownId)
        {
            IEnumerable<string> locks;
            try
            {
                locks = Directory.EnumerateFiles(Root, "*.lock").ToList();
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                return;
            }
            foreach (var lockFile in locks)
            {
                var id = Path.GetFileNameWithoutExtension(lockFile);
                if (id == ownId || !Guid.TryParseExact(id, "N", out _))
                {
                    continue;
                }
                FileStream held;
                try
                {
                    held = new FileStream(lockFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                }
                catch (Exception e) when (e is IOException or UnauthorizedAccessException)
                {
                    // Its process still holds it
                    continue;
                }
                using (held)
                {
                    var folder = Path.Combine(Root, id);
                    if (Directory.Exists(folder))
                    {
                        if (HoldsLink(folder))
                        {
                            _notes.Enqueue((MaintenanceReason.LinkedScratchSkipped, $"The stale scratch folder {folder} is or holds a link and is kept"));
                            continue;
                        }
                        try
                        {
                            Directory.Delete(folder, recursive: true);
                        }
                        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
                        {
                            // Kept with its lock, so a later process tries again
                            continue;
                        }
                    }
                }
                try
                {
                    File.Delete(lockFile);
                }
                catch (Exception e) when (e is IOException or UnauthorizedAccessException)
                {
                }
            }
        }

        // A folder that is, or contains, a symbolic link, junction or other reparse point
        private static bool HoldsLink(string folder)
        {
            var root = new DirectoryInfo(folder);
            if (root.LinkTarget != null || root.Attributes.HasFlag(FileAttributes.ReparsePoint))
            {
                return true;
            }
            var options = new EnumerationOptions() { RecurseSubdirectories = true, AttributesToSkip = 0, IgnoreInaccessible = false };
            try
            {
                return root.EnumerateFileSystemInfos("*", options).Any(x => x.LinkTarget != null || x.Attributes.HasFlag(FileAttributes.ReparsePoint));
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                // What cannot be inspected is not deleted
                return true;
            }
        }

        public DiskLedger Ledger { get; }

        public RootCache Cache { get; }

        // Deletes the test hook refuses, as if the file were held open
        internal Func<string, bool>? DeleteFailsForTests { get; set; }

        public long PendingBytes
        {
            get
            {
                lock (_lock)
                {
                    return _pending.Sum(x => x.Charge);
                }
            }
        }

        public int PendingCount
        {
            get
            {
                lock (_lock)
                {
                    return _pending.Count;
                }
            }
        }

        // An admitted job retries the pending deletions before it reserves anything
        public JobScratch CreateJob()
        {
            RetryPending();
            return new JobScratch(Path.Combine(ProcessPath, "jobs", Guid.NewGuid().ToString("N")), this);
        }

        // Deletes a file; false when it is still there, as with a file another handle holds
        private bool TryDeleteFile(string path)
        {
            if (DeleteFailsForTests?.Invoke(path) == true)
            {
                return false;
            }
            try
            {
                File.Delete(path);
                return true;
            }
            catch (DirectoryNotFoundException)
            {
                // Its folder is gone, so the file is too
                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
        }

        /// <summary>
        /// Deletes a file, then releases its charge; when the file stays, its charge stays too, on the pending list.
        /// </summary>
        public bool DeleteAndRelease(string path, long charge)
        {
            if (TryDeleteFile(path))
            {
                Ledger.Release(charge);
                return true;
            }
            QueuePending(path, charge);
            return false;
        }

        public void QueuePending(string path, long charge)
        {
            lock (_lock)
            {
                _pending.Add((path, charge));
            }
        }

        /// <summary>
        /// Retries every pending deletion; a caller claims the entries, releases each charge after its file is gone, and puts back the rest.
        /// </summary>
        public int RetryPending()
        {
            List<(string Path, long Charge)> claimed;
            lock (_lock)
            {
                if (_pending.Count == 0)
                {
                    return 0;
                }
                claimed = new List<(string, long)>(_pending);
                _pending.Clear();
            }
            var released = 0;
            foreach (var entry in claimed)
            {
                if (DeleteAndRelease(entry.Path, entry.Charge))
                {
                    released++;
                }
            }
            return released;
        }
    }
}
