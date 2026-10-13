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
    /// One job's folder in scratch; each file keeps its disk charge until the file is deleted, or joins the pending list.
    /// </summary>
    internal sealed class JobScratch : IDisposable
    {
        // Charges adopted from writers that charged the ledger themselves, and the quota streams this folder created
        private readonly Dictionary<string, long> _charges = new Dictionary<string, long>(StringComparer.Ordinal);
        private readonly Dictionary<string, QuotaWriteStream> _streams = new Dictionary<string, QuotaWriteStream>(StringComparer.Ordinal);
        private readonly ScratchDirectory? _owner;
        private int _names;

        public JobScratch(string directory, DiskLedger ledger, ScratchDirectory? owner = null)
        {
            DirectoryPath = directory;
            Ledger = ledger;
            _owner = owner;
            Directory.CreateDirectory(directory);
        }

        public string DirectoryPath { get; }

        public DiskLedger Ledger { get; }

        // Files this folder still tracks
        public IReadOnlyCollection<string> Files => _charges.Keys.Union(_streams.Keys).ToList();

        public string NewPath(string suffix) => Path.Combine(DirectoryPath, $"{Interlocked.Increment(ref _names):D6}{suffix}");

        /// <summary>
        /// A new file that charges every growth before it is written.
        /// </summary>
        public QuotaWriteStream Create(string path)
        {
            var file = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 1, FileOptions.None);
            var stream = new QuotaWriteStream(file, Ledger);
            _streams[path] = stream;
            return stream;
        }

        // A file written by a writer that charged the ledger itself
        public void Adopt(string path, long charged)
        {
            _charges[path] = _charges.GetValueOrDefault(path) + charged;
        }

        /// <summary>
        /// Deletes the file, then releases its charge; when the file stays, its charge stays too, on the pending list.
        /// </summary>
        public bool Delete(string path)
        {
            // Windows refuses to delete an open file
            if (_streams.TryGetValue(path, out var stream))
            {
                stream.Dispose();
            }
            var deleted = _owner != null ? _owner.TryDeleteFile(path) : TryDelete(path);
            var charge = Release(path);
            if (deleted)
            {
                Ledger.Release(charge);
            }
            else if (_owner != null)
            {
                _owner.QueuePending(path, charge);
            }
            else
            {
                // Without a process folder the charge stays tracked here
                _charges[path] = charge;
            }
            return deleted;
        }

        private static bool TryDelete(string path)
        {
            try
            {
                File.Delete(path);
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
        /// Stops tracking a file and returns its charge, for a file that leaves the job, such as one moved into the table.
        /// </summary>
        public long Release(string path)
        {
            long charged = 0;
            if (_charges.Remove(path, out var adopted))
            {
                charged += adopted;
            }
            if (_streams.Remove(path, out var stream))
            {
                stream.Dispose();
                charged += stream.Charged;
            }
            return charged;
        }

        public void Dispose()
        {
            foreach (var path in Files)
            {
                Delete(path);
            }
            // The folder's deletion passes the same test hook as its files
            if (_charges.Count == 0 && _streams.Count == 0 && _owner?.DeleteFailsForTests?.Invoke(DirectoryPath) != true)
            {
                try
                {
                    Directory.Delete(DirectoryPath, recursive: false);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }
    }
}
