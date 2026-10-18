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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// What a checked listing may decide.
    /// </summary>
    internal enum EvidencePurpose : byte
    {
        // Absence may authorize deletion; the store must be eligible
        Absence = 0,
        // Only stage files a listed commit positively proves; any store whose table path qualifies
        StageOnly = 1
    }

    /// <summary>
    /// The provider entries of one deletion-evidence listing that passed its checks, in checked order.
    /// </summary>
    internal sealed class EvidenceListing
    {
        private readonly IReadOnlyList<IOEntry> _entries;
        private readonly int _start;

        public EvidenceListing(string folder, IReadOnlyList<IOEntry> entries, int start, EvidencePurpose purpose)
        {
            Folder = folder;
            _entries = entries;
            _start = start;
            Purpose = purpose;
        }

        /// <summary>
        /// The requested folder's full path, slash-terminated.
        /// </summary>
        public string Folder { get; }

        public EvidencePurpose Purpose { get; }

        public int Count => _entries.Count - _start;

        public IOEntry this[int index] => _entries[_start + index];

        /// <summary>
        /// The entry's full path below the folder.
        /// </summary>
        public string RelativeName(int index) => this[index].Path.Full.Substring(Folder.Length);

        public IEnumerable<IOEntry> Entries
        {
            get
            {
                for (int i = _start; i < _entries.Count; i++)
                {
                    yield return _entries[i];
                }
            }
        }
    }

    /// <summary>
    /// A checked listing of a table's log: its commits, checkpoints and cleanup controls; gates take it only once its head's successor was probed absent.
    /// </summary>
    internal sealed class LogEvidence
    {
        // Process-wide probe order
        private static long s_sequence;

        // Commit versions, strictly increasing
        private readonly List<long> _commits;

        private LogEvidence(EvidenceListing entries, DeltaTransactionReader.LogListing listing, List<long> commits)
        {
            Entries = entries;
            Listing = listing;
            _commits = commits;
            Fence = ControlFence.Of(listing);
        }

        public EvidenceListing Entries { get; }

        public EvidencePurpose Purpose => Entries.Purpose;

        /// <summary>
        /// The order in which ReadLogAsync completed the head, fence and successor checks, drawn as its probe started; 0 when they never completed.
        /// </summary>
        public long Sequence { get; private set; }

        /// <summary>
        /// The newest sequence drawn by any probe.
        /// </summary>
        internal static long LastSequence => Interlocked.Read(ref s_sequence);

        /// <summary>
        /// The listing in the form the log readers take, built after the checks so no repeated name was hidden.
        /// </summary>
        public DeltaTransactionReader.LogListing Listing { get; }

        /// <summary>
        /// What this listing's cleanup control records forbid.
        /// </summary>
        public ControlFence Fence { get; }

        /// <summary>
        /// The newest commit JSON, -1 for an empty log; checkpoints and stages never stand in for it.
        /// </summary>
        public long Head => _commits.Count == 0 ? -1 : _commits[^1];

        internal static LogEvidence From(EvidenceListing entries)
        {
            var commits = new List<long>();
            for (int i = 0; i < entries.Count; i++)
            {
                // Folders are skipped as the log classification skips them
                if (!entries[i].Path.IsFolder && DeltaTransactionReader.TryGetCommitVersion(entries[i].Name, out var version))
                {
                    commits.Add(version);
                }
            }
            return new LogEvidence(entries, DeltaTransactionReader.Classify(entries.Entries), commits);
        }

        /// <summary>
        /// Rejects a head below the known published floor or an uninterpreted control record, then probes the head's successor; only when all pass does the listing get its sequence.
        /// </summary>
        internal async Task<ListingCheck> CheckCurrentAsync(IFileStorage storage, IOPath table, long publishedFloor, CancellationToken cancellationToken)
        {
            // A checkpoint or other version file above the newest JSON implies commits the listing does not show
            if (Head < publishedFloor || Listing.Head > Head || Listing.MaxOtherVersion > Head)
            {
                return ListingCheck.RangeGap;
            }
            if (!Fence.AllowsDeletion)
            {
                return ListingCheck.ControlFenced;
            }
            // Drawn between the listing and the probe
            var sequence = Interlocked.Increment(ref s_sequence);
            var probe = await ListingEvidence.ProbeSuccessor(storage, table, Head, cancellationToken);
            if (probe == ListingCheck.Passed)
            {
                Sequence = sequence;
            }
            return probe;
        }

        /// <summary>
        /// Whether every commit JSON in [from, to] is listed; an empty range passes.
        /// </summary>
        public ListingCheck CheckRange(long from, long to)
        {
            if (from > to)
            {
                return ListingCheck.Passed;
            }
            var index = _commits.BinarySearch(from);
            // Strictly increasing integers are contiguous exactly when the distance in versions equals the distance in positions, which also bounds the range by the head
            if (index < 0 || to - from >= _commits.Count - index)
            {
                return ListingCheck.RangeGap;
            }
            return _commits[index + (int)(to - from)] == to ? ListingCheck.Passed : ListingCheck.RangeGap;
        }
    }

    /// <summary>
    /// One listed stage file and the listed commit at the version it stages.
    /// </summary>
    internal readonly record struct ListedStage(IOEntry Stage, LogTransactionFile Target)
    {
        public long Version => Target.Version;
    }

    /// <summary>
    /// A checked log listing that supports only positively proven stage-file removal: it shows stages whose commit is listed and nothing payload, temporary, control or absence decisions could use.
    /// </summary>
    internal sealed class StageEvidence
    {
        private readonly LogEvidence _log;

        internal StageEvidence(LogEvidence log)
        {
            _log = log;
            var stages = new List<ListedStage>();
            var entries = log.Entries;
            for (int i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                // A stage whose commit is not listed proves nothing, so it is not shown
                if (!entry.Path.IsFolder && ListingEvidence.TryGetStageVersion(entry.Name, out var version) && log.Listing.Commits.TryGetValue(version, out var target))
                {
                    stages.Add(new ListedStage(entry, target));
                }
            }
            Stages = stages;
        }

        /// <summary>
        /// The newest commit JSON, -1 for an empty log.
        /// </summary>
        public long Head => _log.Head;

        // Listed stages with listed commits, in checked order
        public IReadOnlyList<ListedStage> Stages { get; }

        /// <summary>
        /// A listed commit; null proves nothing about absence.
        /// </summary>
        public LogTransactionFile? ListedCommit(long version) => _log.Listing.Commits.TryGetValue(version, out var commit) ? commit : null;

        /// <summary>
        /// Whether every commit JSON in [from, to] is listed; an empty range passes.
        /// </summary>
        public ListingCheck CheckRange(long from, long to) => _log.CheckRange(from, to);
    }

    /// <summary>
    /// The checks every listing must pass before it may decide a deletion: delivery model, containment, order, uniqueness, relied-on ranges and successor absence.
    /// </summary>
    internal static class ListingEvidence
    {
        // Estimated managed bytes per remembered synthetic folder, its string and set slot
        private const long FolderStateBytes = 64;

        // {version:D20}.json
        private const int CommitNameLength = 25;
        private const string StageSuffix = ".tmp";

        /// <summary>
        /// Checks one listing as delivered for a purpose, before any filtering; an unpaged snapshot is sorted in place, so the caller hands over the collection.
        /// </summary>
        public static ListingCheck Check(CleanupStoreContract store, IOPath folder, bool recursive, IReadOnlyCollection<IOEntry> delivered, out EvidenceListing? listing, JobMemory? memory = null,
            EvidencePurpose purpose = EvidencePurpose.Absence)
        {
            listing = null;
            if (!Admits(store, purpose))
            {
                return ListingCheck.StoreIneligible;
            }
            var folderFull = folder.WTS;
            var delivery = store.Store.Delivery;
            if (delivery == ListingDelivery.PinnedCloud && !QualifiesFolder(store, folder))
            {
                return ListingCheck.StoreIneligible;
            }
            var entries = delivered as IReadOnlyList<IOEntry> ?? delivered.ToList();
            var start = 0;
            if (delivery == ListingDelivery.PinnedCloud && recursive)
            {
                var envelope = StripSyntheticFolders(store.Container!, folder, entries, memory, out start);
                if (envelope != ListingCheck.Passed)
                {
                    return envelope;
                }
            }
            else if (delivery == ListingDelivery.UnpagedSnapshot)
            {
                // Native enumeration has no order and no pages; repeated names stay for the uniqueness check
                var sorted = entries as List<IOEntry> ?? entries.ToList();
                sorted.Sort(static (left, right) => CompareUtf8(left.Path.Full, right.Path.Full));
                entries = sorted;
            }
            // Azure lists a hierarchical namespace with slash lowest; either documented order must hold for the whole sequence
            var dualOrder = delivery == ListingDelivery.PinnedCloud && recursive && store.Store.Kind == StoreKind.Azure;
            var ordinary = true;
            var slashLowest = dualOrder;
            string? previous = null;
            for (int i = start; i < entries.Count; i++)
            {
                var entry = entries[i];
                var full = entry.Path.Full;
                if (full.Length <= folderFull.Length || !full.StartsWith(folderFull, StringComparison.Ordinal))
                {
                    return ListingCheck.Containment;
                }
                if (!recursive && (IsDescendant(full, folderFull.Length) || (entry.Path.IsFolder && delivery != ListingDelivery.UnpagedSnapshot)))
                {
                    return ListingCheck.UnsupportedSubfolder;
                }
                if (previous != null)
                {
                    ordinary &= CompareUtf8(previous, full) < 0;
                    slashLowest &= CompareSlashLowest(previous, full) < 0;
                    if (!ordinary && !slashLowest)
                    {
                        return ListingCheck.OrderOrDuplicate;
                    }
                }
                previous = full;
            }
            listing = new EvidenceListing(folderFull, entries, start, purpose);
            return ListingCheck.Passed;
        }

        /// <summary>
        /// Lists the table's log through the current storage for absence evidence, checks it, rejects a head below the known published floor or an uninterpreted control record, and probes the head's successor; a listing error propagates as a transient failure and a stop as cancellation.
        /// </summary>
        public static Task<(ListingCheck Check, LogEvidence? Log)> ReadLogAsync(CleanupConfiguration configuration, IFileStorage current, long publishedFloor, CancellationToken cancellationToken, JobMemory? memory = null)
        {
            return ReadAsync(configuration, current, publishedFloor, EvidencePurpose.Absence, cancellationToken, memory);
        }

        /// <summary>
        /// Lists the table's log for positively proven stage-file removal with every check ReadLogAsync makes, on an ineligible store too when its table path qualifies.
        /// </summary>
        public static async Task<(ListingCheck Check, StageEvidence? Stages)> ReadStagesAsync(CleanupConfiguration configuration, IFileStorage current, long publishedFloor, CancellationToken cancellationToken,
            JobMemory? memory = null)
        {
            var (check, log) = await ReadAsync(configuration, current, publishedFloor, EvidencePurpose.StageOnly, cancellationToken, memory);
            return log == null ? (check, null) : (check, new StageEvidence(log));
        }

        private static async Task<(ListingCheck Check, LogEvidence? Log)> ReadAsync(CleanupConfiguration configuration, IFileStorage current, long publishedFloor, EvidencePurpose purpose,
            CancellationToken cancellationToken, JobMemory? memory)
        {
            if (configuration.StorageChanged(current))
            {
                return (ListingCheck.StorageChanged, null);
            }
            var store = configuration.Store;
            if (!Admits(store, purpose))
            {
                return (ListingCheck.StoreIneligible, null);
            }
            var folder = store.Table.Combine(DeltaTransactionReader.DeltaLogDirName);
            var delivered = await current.Ls(folder, recurse: false, cancellationToken);
            var check = Check(store, folder, false, delivered, out var listing, memory, purpose);
            if (check != ListingCheck.Passed)
            {
                return (check, null);
            }
            var log = LogEvidence.From(listing!);
            var probed = await log.CheckCurrentAsync(current, store.Table, publishedFloor, cancellationToken);
            return probed == ListingCheck.Passed ? (ListingCheck.Passed, log) : (probed, null);
        }

        // Absence needs an eligible store; positive stage evidence needs only a table path that qualifies
        private static bool Admits(CleanupStoreContract store, EvidencePurpose purpose)
        {
            return purpose == EvidencePurpose.Absence ? store.Eligible : store.PathProblem == null;
        }

        /// <summary>
        /// A direct Stat of the exact commit after the head; only a valid absence passes, an empty log probes version 0, and a stop of the token propagates as cancellation.
        /// </summary>
        public static async Task<ListingCheck> ProbeSuccessor(IFileStorage storage, IOPath table, long head, CancellationToken cancellationToken)
        {
            if (head == long.MaxValue)
            {
                return ListingCheck.Overflow;
            }
            if (head < -1)
            {
                return ListingCheck.ProbeError;
            }
            var successor = table.Combine(DeltaTransactionReader.DeltaLogDirName).Combine((head + 1).ToString("D20", CultureInfo.InvariantCulture) + ".json");
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var entry = await storage.Stat(successor, cancellationToken);
                return entry == null ? ListingCheck.Passed : ListingCheck.SuccessorPresent;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The job stopped, which is not a failed probe
                throw;
            }
            catch
            {
                // Errors and cancellations the stop did not cause are not absence
                return ListingCheck.ProbeError;
            }
        }

        /// <summary>
        /// The version a stage file name stages: .{version:D20}.json.{id}.tmp with a nonempty id.
        /// </summary>
        public static bool TryGetStageVersion(string name, out long version)
        {
            version = -1;
            if (name.Length < 1 + CommitNameLength + 1 + 1 + StageSuffix.Length || name[0] != '.' || name[1 + CommitNameLength] != '.' || !name.EndsWith(StageSuffix, StringComparison.Ordinal))
            {
                return false;
            }
            return DeltaTransactionReader.TryGetCommitVersion(name.Substring(1, CommitNameLength), out version);
        }

        // The requested folder lies in the table's bucket or container under a qualified prefix
        private static bool QualifiesFolder(CleanupStoreContract store, IOPath folder)
        {
            try
            {
                folder.ExtractPrefixAndRelativePath(out var container, out var relative);
                return container == store.Container && store.QualifiesPrefix(relative.NLWTS);
            }
            catch
            {
                return false;
            }
        }

        // A nonrecursive listing holds only direct children
        private static bool IsDescendant(string full, int folderLength)
        {
            var end = full[^1] == '/' ? full.Length - 1 : full.Length;
            return full.IndexOf('/', folderLength, end - folderLength) >= 0;
        }

        // The shape of a folder the pinned client invents: slash-terminated with no size
        private static bool IsSyntheticShape(IOEntry entry)
        {
            return entry.Path.IsFolder && entry.Size == null;
        }

        /// <summary>
        /// Recomputes the folder block a pinned recursive S3 or Azure listing puts first and requires an exact ordered match; only that block is skipped.
        /// </summary>
        private static ListingCheck StripSyntheticFolders(string container, IOPath folder, IReadOnlyList<IOEntry> entries, JobMemory? memory, out int start)
        {
            start = 0;
            var block = 0;
            while (block < entries.Count && IsSyntheticShape(entries[block]))
            {
                block++;
            }
            long charged = 0;
            try
            {
                // The same steps as the client: strip the normalized root, take the parent, drop root parents, keep first occurrences, then prefix the container
                folder.ExtractPrefixAndRelativePath(out _, out var relative);
                var root = IOPath.Normalize(relative.Full, false, false);
                var containerPath = new IOPath(container);
                var containerFolder = IOPath.PathSeparatorString + container + IOPath.PathSeparatorString;
                var seen = new HashSet<string>(StringComparer.Ordinal);
                var matched = 0;
                for (int i = block; i < entries.Count; i++)
                {
                    // A provider never delivers this shape, so it is a synthetic folder out of place
                    if (IsSyntheticShape(entries[i]))
                    {
                        return ListingCheck.EnvelopeMismatch;
                    }
                    var full = entries[i].Path.Full;
                    if (!full.StartsWith(containerFolder, StringComparison.Ordinal))
                    {
                        return ListingCheck.Containment;
                    }
                    var unprefixed = full.Substring(container.Length + 1);
                    // Shorter than the root it was listed under, so outside the folder
                    if (unprefixed.Length < root.Length)
                    {
                        return ListingCheck.Containment;
                    }
                    var parent = IOPath.GetParent(unprefixed.Substring(root.Length));
                    if (IOPath.IsRoot(parent!) || !seen.Add(parent!))
                    {
                        continue;
                    }
                    var bytes = FolderStateBytes + 2L * (parent?.Length ?? 0);
                    memory?.Reserve(bytes);
                    charged += bytes;
                    if (matched >= block || !IsSyntheticFolder(entries[matched], parent, containerPath))
                    {
                        return ListingCheck.EnvelopeMismatch;
                    }
                    matched++;
                }
                if (matched != block)
                {
                    return ListingCheck.EnvelopeMismatch;
                }
                start = block;
                return ListingCheck.Passed;
            }
            catch (CheckpointDeclinedException)
            {
                throw;
            }
            catch
            {
                return ListingCheck.EnvelopeMismatch;
            }
            finally
            {
                memory?.Release(charged);
            }
        }

        // Exactly the entry the client creates for a parent folder and then prefixes with the container
        private static bool IsSyntheticFolder(IOEntry actual, string? parent, IOPath container)
        {
            var expected = new IOEntry(parent + IOPath.PathSeparatorString);
            return actual.Path.Full == expected.Path.Prefix(container)?.Full
                && actual.Size == expected.Size
                && actual.MD5 == expected.MD5
                && actual.CreatedTime == expected.CreatedTime
                && actual.LastModificationTime == expected.LastModificationTime
                && actual.Properties.Count == expected.Properties.Count
                && actual.Metadata.Count == expected.Metadata.Count
                && actual.Tag == expected.Tag;
        }

        /// <summary>
        /// UTF-8 byte order, which is code point order: S3's documented listing order.
        /// </summary>
        internal static int CompareUtf8(string left, string right)
        {
            var length = Math.Min(left.Length, right.Length);
            for (int i = 0; i < length; i++)
            {
                if (left[i] != right[i])
                {
                    return CodePointRank(left[i]) - CodePointRank(right[i]);
                }
            }
            return left.Length - right.Length;
        }

        /// <summary>
        /// The hierarchical namespace order: UTF-8 byte order with the slash below every other character.
        /// </summary>
        internal static int CompareSlashLowest(string left, string right)
        {
            var length = Math.Min(left.Length, right.Length);
            for (int i = 0; i < length; i++)
            {
                if (left[i] != right[i])
                {
                    return SlashLowestRank(left[i]) - SlashLowestRank(right[i]);
                }
            }
            return left.Length - right.Length;
        }

        // Surrogates encode code points above U+FFFF, so they rank above U+E000-U+FFFF
        private static int CodePointRank(char c)
        {
            return c >= 0xD800 ? (c >= 0xE000 ? c - 0x800 : c + 0x2000) : c;
        }

        private static int SlashLowestRank(char c)
        {
            return c == '/' ? -1 : CodePointRank(c);
        }
    }
}
