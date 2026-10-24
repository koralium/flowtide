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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Stowage;
using Stowage.Impl;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion
{
    /// <summary>
    /// What a deletion target is, which decides where it may lie and how many of its deletes may be outstanding.
    /// </summary>
    internal enum TargetKind : byte
    {
        // A data, deletion vector or change data file outside every log
        Payload = 0,
        // A staged commit directly in the table's log
        Stage = 1,
        // A checkpoint publication temporary directly in the table's log
        Temporary = 2,
        // A cleanup control record directly in the table's log
        Control = 3,
        // A versioned file directly in the table's log, deleted one at a time
        Log = 4
    }

    /// <summary>
    /// One object to delete: its exact key, the full storage path on object stores and the native path on local disk, and its kind.
    /// </summary>
    internal readonly record struct DeletionTarget(string Key, TargetKind Kind)
    {
        // The size an inventory listed, null when unknown; pinned Azure orphan runs need it positive
        public long? Size { get; init; }

        // The modification time an inventory listed, null when unknown
        public DateTimeOffset? Modified { get; init; }

        // The MD5 an inventory listed, null when unknown
        public string? Md5 { get; init; }

        // A planned path a foreign change referenced again, passed without a delete
        public bool Excluded { get; init; }
    }

    /// <summary>
    /// Why a target was kept instead of deleted, a finite set.
    /// </summary>
    internal enum KeepReason : byte
    {
        None = 0,
        // The storage is not the instance classified at construction
        StorageChanged = 1,
        // The store cannot delete exactly one named object
        StoreUnsupported = 2,
        // The key is not this table's exact qualified name for an object
        Unqualified = 3,
        // A folder or a folder placeholder
        Folder = 4,
        // A payload inside a log, or a log file outside the table's log or not named as its kind
        Misplaced = 5,
        // A directory on disk, or a folder an unknown stack's or the Azure submission probe's Stat reports
        Directory = 6,
        // A reparse point at the table root, between it and the target, or the target itself
        ReparsePoint = 7,
        // A short alias or another spelling that names a different long path
        Alias = 8,
        // A size unknown or not positive where a delete needs a positive one: a pinned Azure orphan, or any target on an unknown stack
        UnknownSize = 9,
        // An excluded target, which a run passes
        Excluded = 10,
        // A pinned Azure orphan whose size, modification time or MD5 differs from its inventory's
        IdentityChanged = 11,
        // A pinned Azure orphan whose submission probe failed
        ProbeError = 12
    }

    internal enum DeleteStatus : byte
    {
        Deleted,
        // Nothing was there to delete
        Absent,
        Kept,
        // The delete was attempted and failed or was cancelled, so its effect is uncertain
        Failed
    }

    internal readonly record struct DeleteOutcome(DeleteStatus Status, KeepReason Reason = KeepReason.None)
    {
        // For logs, never a metric label
        public Exception? Error { get; init; }

        public bool Done => Status is DeleteStatus.Deleted or DeleteStatus.Absent;

        // Done, or kept only for its exclusion
        public bool Passes => Done || Reason == KeepReason.Excluded;

        public static DeleteOutcome Keep(KeepReason reason) => new DeleteOutcome(DeleteStatus.Kept, reason);

        public static DeleteOutcome Fail(Exception error) => new DeleteOutcome(DeleteStatus.Failed) { Error = error };
    }

    /// <summary>
    /// Counts of what deletions did; concurrent operations add to it.
    /// </summary>
    internal sealed class DeletionTally
    {
        private readonly long[] _kept = new long[(int)KeepReason.ProbeError + 1];
        private long _deleted;
        private long _absent;
        private long _failed;
        private long _leaked;

        public long Deleted => Interlocked.Read(ref _deleted);

        public long Absent => Interlocked.Read(ref _absent);

        public long Failed => Interlocked.Read(ref _failed);

        // Failed volatile targets a run left behind and passed after its retries
        public long Leaked => Interlocked.Read(ref _leaked);

        public void AddLeaked() => Interlocked.Increment(ref _leaked);

        public long Kept
        {
            get
            {
                long kept = 0;
                for (int i = 0; i < _kept.Length; i++)
                {
                    kept += Interlocked.Read(ref _kept[i]);
                }
                return kept;
            }
        }

        public long KeptFor(KeepReason reason) => Interlocked.Read(ref _kept[(int)reason]);

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
                    Interlocked.Increment(ref _kept[(int)outcome.Reason]);
                    break;
                default:
                    Interlocked.Increment(ref _failed);
                    break;
            }
        }
    }

    /// <summary>
    /// Deletes single objects of one table by their exact names only: object keys through the store's qualified domain, local files with File.Delete after rechecking containment, reparse points, directories and aliases.
    /// </summary>
    internal sealed class DeletionExecutor
    {
        private const string LogFolder = "_delta_log";
        // Hadoop's empty-folder marker suffix
        private const string PlaceholderSuffix = "$folder$";
        private const string TemporaryPrefix = ".flowtide-";
        private static readonly string[] s_temporarySuffixes = { ".checkpoint.tmp", ".last_checkpoint.tmp" };
        private static readonly string[] s_devices = { "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$" };
        // A key's name follows its last separator, native or object
        private static readonly char[] s_separators = { '/', Path.DirectorySeparatorChar };

        // The table's directory in the store's own native form; null when the store is not local disk or the form fails
        private readonly string? _nativeRoot;

        public DeletionExecutor(CleanupConfiguration configuration, IFileStorage storage)
        {
            Configuration = configuration;
            Storage = storage;
            if (Native)
            {
                _nativeRoot = NativeRootOf(configuration.Store);
            }
        }

        public CleanupConfiguration Configuration { get; }

        // Background runs pass the maintenance meter; the sink's inline fallback passes its own storage
        public IFileStorage Storage { get; }

        public DeletionTally Tally { get; } = new DeletionTally();

        // Runs after every check passed, immediately before the delete is submitted
        internal Func<DeletionTarget, Task>? BeforeDeleteForTests { get; set; }

        // Called with each target's outcome once DeleteAsync counted it
        internal Action<DeletionTarget, DeleteOutcome>? Settled { get; init; }

        // Called with each target a run keeps before submitting its delete
        internal Action<DeletionTarget, DeleteOutcome>? Withheld { get; init; }

        // Replaces Windows long path resolution, since a short alias without '~' needs a privilege to create
        internal Func<string, string?>? LongPathForTests { get; set; }

        // Local disk is deleted natively, because its Rm deletes a directory recursively
        private bool Native => Configuration.Store.Store.Kind == StoreKind.LocalDisk;

        /// <summary>
        /// Whether the storage this executor deletes through, or the sink's storage now, is not the instance classified at construction; never throws.
        /// </summary>
        public bool StorageChanged => Configuration.StorageChanged(Storage) || Configuration.StorageReplaced();

        /// <summary>
        /// The exact target for a table-relative name in this store's own form; the result is the reason it must be kept, None when it may be submitted.
        /// </summary>
        public KeepReason TryTarget(string tableRelative, TargetKind kind, out DeletionTarget target)
        {
            target = default;
            if (!Configuration.Store.TryQualifyTarget(tableRelative, out var path))
            {
                return StoreUnsupported(Configuration.Store) ? KeepReason.StoreUnsupported : KeepReason.Unqualified;
            }
            var key = Native && _nativeRoot != null ? _nativeRoot + Path.DirectorySeparatorChar + tableRelative.Replace('/', Path.DirectorySeparatorChar) : path.Full;
            target = new DeletionTarget(key, kind);
            return Check(target);
        }

        /// <summary>
        /// The target of a candidate's exact deletion path, the full store path a path key carries, in this store's own form; any other path is handed on as given for the checks to keep.
        /// </summary>
        public DeletionTarget TargetOf(string deletionPath, TargetKind kind = TargetKind.Payload)
        {
            var prefix = Configuration.Store.Table.Full + IOPath.PathSeparatorString;
            if (deletionPath.StartsWith(prefix, StringComparison.Ordinal))
            {
                TryTarget(deletionPath.Substring(prefix.Length), kind, out var target);
                if (target.Key != null)
                {
                    return target;
                }
            }
            return new DeletionTarget(deletionPath, kind);
        }

        /// <summary>
        /// The characters a target's key holds before its table-relative name: the native root and a separator on local disk, the table path and a slash elsewhere.
        /// </summary>
        public static int KeyPrefixLength(CleanupConfiguration configuration)
        {
            var root = configuration.Store.Store.Kind == StoreKind.LocalDisk ? NativeRootOf(configuration.Store) : null;
            return (root ?? configuration.Store.Table.Full).Length + 1;
        }

        /// <summary>
        /// The target of a candidate path key; a key without a deletion path names no target.
        /// </summary>
        public DeletionTarget TargetOf(in PathKey key, TargetKind kind = TargetKind.Payload) => TargetOf(key.DeletionPath ?? string.Empty, kind);

        /// <summary>
        /// Why a target must be kept, judged from its key, kind and exclusion alone; None when it may be submitted. Makes no request.
        /// </summary>
        public KeepReason Check(DeletionTarget target)
        {
            if (StorageChanged)
            {
                return KeepReason.StorageChanged;
            }
            if (StoreUnsupported(Configuration.Store))
            {
                return KeepReason.StoreUnsupported;
            }
            if (string.IsNullOrEmpty(target.Key))
            {
                return KeepReason.Unqualified;
            }
            var relative = Native ? NativeRelative(target.Key) : ObjectRelative(target.Key);
            var placed = relative == null ? KeepReason.Unqualified : Placement(relative, target.Kind);
            // Only a target that could be deleted is passed for its exclusion
            return placed == KeepReason.None && target.Excluded ? KeepReason.Excluded : placed;
        }

        /// <summary>
        /// Counts a target a run keeps before submitting its delete.
        /// </summary>
        public DeleteOutcome Withhold(DeletionTarget target, KeepReason reason)
        {
            var kept = DeleteOutcome.Keep(reason);
            Tally.Add(kept);
            Withheld?.Invoke(target, kept);
            return kept;
        }

        /// <summary>
        /// Deletes one target when every check passes; never throws, so a failure or a stop is returned as Failed and counted.
        /// </summary>
        public async Task<DeleteOutcome> DeleteAsync(DeletionTarget target, bool orphan, CancellationToken cancellationToken)
        {
            DeleteOutcome outcome;
            try
            {
                var reason = Check(target);
                if (reason != KeepReason.None)
                {
                    outcome = DeleteOutcome.Keep(reason);
                }
                else if (Native)
                {
                    outcome = await DeleteNativeAsync(target, cancellationToken);
                }
                else
                {
                    outcome = await DeleteObjectAsync(target, orphan, cancellationToken);
                }
            }
            catch (Exception e)
            {
                outcome = DeleteOutcome.Fail(e);
            }
            Tally.Add(outcome);
            Settled?.Invoke(target, outcome);
            return outcome;
        }

        /// <summary>
        /// Deletes one of the producing attempt's own discarded outputs by its exact name and as a file only; on an unvouched unknown stack only a qualified key whose Stat shows a file of positive size goes, and any other is kept. Never throws.
        /// </summary>
        public async Task<DeleteOutcome> DeleteOwnAsync(string tableRelative, CancellationToken cancellationToken)
        {
            DeleteOutcome outcome;
            DeletionTarget target = default;
            try
            {
                var reason = TryTarget(tableRelative, TargetKind.Payload, out target);
                if (reason == KeepReason.None)
                {
                    return await DeleteAsync(target, false, cancellationToken);
                }
                // Unsupported only for want of the assertion
                if (reason == KeepReason.StoreUnsupported && Configuration.Store is { Asserted: false, PathProblem: null, Store: { Kind: StoreKind.Unknown, ExactDeletes: true } })
                {
                    reason = UnvouchedTarget(tableRelative, out target);
                }
                // An unknown stack's object delete Stats the key first
                outcome = reason != KeepReason.None ? DeleteOutcome.Keep(reason) : await DeleteObjectAsync(target, false, cancellationToken, unvouched: true);
            }
            catch (Exception e)
            {
                outcome = DeleteOutcome.Fail(e);
            }
            Tally.Add(outcome);
            if (target.Key != null)
            {
                Settled?.Invoke(target, outcome);
            }
            return outcome;
        }

        // An own output's full key on an unvouched stack: nonempty segments of the qualified characters, which hold no space, none ending in a dot, outside every log
        private KeepReason UnvouchedTarget(string tableRelative, out DeletionTarget target)
        {
            target = default;
            if (StorageChanged)
            {
                return KeepReason.StorageChanged;
            }
            var full = Configuration.Store.Table.Full + IOPath.PathSeparatorString + tableRelative;
            if (CleanupStoreContract.CheckSegments(tableRelative, true) != null || CleanupStoreContract.HasTrailingDotSegment(full))
            {
                return KeepReason.Unqualified;
            }
            target = new DeletionTarget(full, TargetKind.Payload);
            return Placement(tableRelative, TargetKind.Payload);
        }

        // The same conditions under which the contract qualifies no target at all
        private static bool StoreUnsupported(CleanupStoreContract store)
        {
            return store.PathProblem != null || !store.Store.ExactDeletes || (store.Store.Kind == StoreKind.Unknown && !store.Asserted);
        }

        // The table-relative name of an exact qualified key of this table, null for any other key
        private string? ObjectRelative(string key)
        {
            var table = Configuration.Store.Table.Full;
            if (key.Length <= table.Length + 1 || !key.StartsWith(table, StringComparison.Ordinal) || key[table.Length] != '/')
            {
                return null;
            }
            var relative = key.Substring(table.Length + 1);
            // The whole key is in the transmission domain, so the client sends exactly this name and never a shorter one
            return Configuration.Store.TryQualifyTarget(relative, out _) ? relative : null;
        }

        // The slash-separated name below the table root of a canonical native path inside it, null for any other path
        private string? NativeRelative(string key)
        {
            var root = _nativeRoot;
            if (root == null || !string.Equals(FullPath(key), key, StringComparison.Ordinal))
            {
                return null;
            }
            if (key.Length <= root.Length + 1 || !key.StartsWith(root, StringComparison.Ordinal) || key[root.Length] != Path.DirectorySeparatorChar)
            {
                return null;
            }
            var relative = key.Substring(root.Length + 1);
            if (OperatingSystem.IsWindows())
            {
                if (UnsupportedWindowsName(relative))
                {
                    return null;
                }
                return relative.Replace(Path.DirectorySeparatorChar, '/');
            }
            return relative;
        }

        private static string? FullPath(string path)
        {
            try
            {
                return Path.GetFullPath(path);
            }
            catch
            {
                return null;
            }
        }

        // Stream syntax, a segment Windows would trim, or a device name
        private static bool UnsupportedWindowsName(string relative)
        {
            foreach (var segment in relative.Split(Path.DirectorySeparatorChar))
            {
                if (segment.Contains(':') || (segment.Length > 0 && (segment[^1] == '.' || segment[^1] == ' ')))
                {
                    return true;
                }
                var dot = segment.IndexOf('.');
                var stem = (dot < 0 ? segment : segment.Substring(0, dot)).TrimEnd(' ');
                if (IsDevice(stem))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsDevice(string stem)
        {
            foreach (var device in s_devices)
            {
                if (stem.Equals(device, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            // COM0-9 and LPT0-9, including the superscript digits Windows also reserves
            return stem.Length == 4 && (stem.StartsWith("COM", StringComparison.OrdinalIgnoreCase) || stem.StartsWith("LPT", StringComparison.OrdinalIgnoreCase))
                && (char.IsAsciiDigit(stem[3]) || stem[3] is '\u00B9' or '\u00B2' or '\u00B3');
        }

        // Where a kind may lie: payload never in a log, every other kind directly in the table's log under its own naming
        private static KeepReason Placement(string relative, TargetKind kind)
        {
            var slash = relative.LastIndexOf('/');
            var name = relative.Substring(slash + 1);
            if (name.Length == 0 || name.EndsWith(PlaceholderSuffix, StringComparison.OrdinalIgnoreCase))
            {
                return KeepReason.Folder;
            }
            if (kind == TargetKind.Payload)
            {
                // A nested table's log too, in any case
                foreach (var segment in relative.Split('/'))
                {
                    if (segment.Equals(LogFolder, StringComparison.OrdinalIgnoreCase))
                    {
                        return KeepReason.Misplaced;
                    }
                }
                return KeepReason.None;
            }
            if (slash != LogFolder.Length || !relative.StartsWith(LogFolder, StringComparison.Ordinal))
            {
                return KeepReason.Misplaced;
            }
            var named = kind switch
            {
                TargetKind.Stage => ListingEvidence.TryGetStageVersion(name, out _),
                TargetKind.Control => name.Length > DeltaTransactionReader.CleanupControlPrefix.Length && name.StartsWith(DeltaTransactionReader.CleanupControlPrefix, StringComparison.Ordinal),
                TargetKind.Temporary => IsTemporary(name),
                TargetKind.Log => IsVersioned(name),
                _ => false
            };
            return named ? KeepReason.None : KeepReason.Misplaced;
        }

        // .flowtide-{guid:N}.checkpoint.tmp or .flowtide-{guid:N}.last_checkpoint.tmp
        internal static bool IsTemporary(string name)
        {
            if (!name.StartsWith(TemporaryPrefix, StringComparison.Ordinal))
            {
                return false;
            }
            foreach (var suffix in s_temporarySuffixes)
            {
                if (name.Length == TemporaryPrefix.Length + 32 + suffix.Length && name.EndsWith(suffix, StringComparison.Ordinal))
                {
                    return Guid.TryParseExact(name.AsSpan(TemporaryPrefix.Length, 32), "N", out _);
                }
            }
            return false;
        }

        // {version:D20}. followed by the rest of a commit, checksum, compaction or checkpoint name
        private static bool IsVersioned(string name)
        {
            return name.Length > 21 && name[20] == '.' && IsVersion(name.AsSpan(0, 20));
        }

        private static bool IsVersion(ReadOnlySpan<char> digits)
        {
            foreach (var c in digits)
            {
                if (!char.IsAsciiDigit(c))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// The highest version a log target's name carries, a compaction's end version included; long.MaxValue past a long, null for a name without one.
        /// </summary>
        internal static long? LogVersion(DeletionTarget target)
        {
            var key = target.Key ?? string.Empty;
            var name = key.Substring(key.LastIndexOfAny(s_separators) + 1);
            if (!IsVersioned(name))
            {
                return null;
            }
            var version = Parse(name.AsSpan(0, 20));
            // {start:D20}.{end:D20}.compacted.json
            if (name.Length > 41 && name[41] == '.' && IsVersion(name.AsSpan(21, 20)))
            {
                version = Math.Max(version, Parse(name.AsSpan(21, 20)));
            }
            return version;
        }

        /// <summary>
        /// The version a log or stage target may not be issued above: a log name's highest version or a stage's own; null for other kinds and names without one.
        /// </summary>
        internal static long? BoundVersion(DeletionTarget target)
        {
            switch (target.Kind)
            {
                case TargetKind.Log:
                    return LogVersion(target);
                case TargetKind.Stage:
                    var key = target.Key ?? string.Empty;
                    return ListingEvidence.TryGetStageVersion(key.Substring(key.LastIndexOfAny(s_separators) + 1), out var version) ? version : null;
                default:
                    return null;
            }
        }

        private static long Parse(ReadOnlySpan<char> digits) => long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var value) ? value : long.MaxValue;

        // A 16-byte MD5 from the hex the pinned Stat reports or the base64 its listing reports; null for any other text
        private static UInt128? Md5Of(string? text)
        {
            if (text is { Length: 32 } && UInt128.TryParse(text, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var hex))
            {
                return hex;
            }
            Span<byte> bytes = stackalloc byte[16];
            return text != null && Convert.TryFromBase64String(text, bytes, out var written) && written == bytes.Length ? BinaryPrimitives.ReadUInt128BigEndian(bytes) : null;
        }

        // Unvouched: nobody vouched that a Stat showing nothing means a missing object
        private async Task<DeleteOutcome> DeleteObjectAsync(DeletionTarget target, bool orphan, CancellationToken cancellationToken, bool unvouched = false)
        {
            var path = new IOPath(target.Key);
            var kind = Configuration.Store.Store.Kind;
            if (orphan && kind == StoreKind.Azure)
            {
                // The pinned client reports hierarchical-namespace directories as files of size zero
                if (target.Size is not > 0)
                {
                    return DeleteOutcome.Keep(KeepReason.UnknownSize);
                }
                IOEntry? entry;
                try
                {
                    entry = await Storage.Stat(path, cancellationToken);
                }
                catch
                {
                    return DeleteOutcome.Keep(KeepReason.ProbeError);
                }
                if (entry == null)
                {
                    return new DeleteOutcome(DeleteStatus.Absent);
                }
                if (entry.Path.IsFolder)
                {
                    return DeleteOutcome.Keep(KeepReason.Directory);
                }
                if (entry.Size is not > 0)
                {
                    return DeleteOutcome.Keep(KeepReason.UnknownSize);
                }
                // Identity frozen at the inventory: size always, time and MD5 when both sides know them
                if (entry.Size != target.Size || (target.Modified is { } frozen && entry.LastModificationTime is { } modified && modified != frozen)
                    || (Md5Of(target.Md5) is { } frozenMd5 && Md5Of(entry.MD5) is { } probedMd5 && probedMd5 != frozenMd5))
                {
                    return DeleteOutcome.Keep(KeepReason.IdentityChanged);
                }
            }
            else if (kind == StoreKind.Unknown)
            {
                // An unknown stack's Rm may remove a folder with everything in it, so only an existing file of known positive size goes
                var entry = await Storage.Stat(path, cancellationToken);
                if (entry == null)
                {
                    return unvouched ? DeleteOutcome.Keep(KeepReason.UnknownSize) : new DeleteOutcome(DeleteStatus.Absent);
                }
                if (entry.Path.IsFolder)
                {
                    return DeleteOutcome.Keep(KeepReason.Directory);
                }
                if (entry.Size is not > 0)
                {
                    return DeleteOutcome.Keep(KeepReason.UnknownSize);
                }
            }
            if (BeforeDeleteForTests is { } hook)
            {
                await hook(target);
            }
            await Storage.Rm(path, cancellationToken);
            return new DeleteOutcome(DeleteStatus.Deleted);
        }

        private async Task<DeleteOutcome> DeleteNativeAsync(DeletionTarget target, CancellationToken cancellationToken)
        {
            var root = _nativeRoot!;
            var key = target.Key;
            // The checks and the delete are one native request
            await MeteredFileStorage.ChargeNativeAsync(Storage, 1, 0, cancellationToken);

            // The root and every directory down to the target, none of them a reparse point
            var directory = root;
            var relative = key.Substring(root.Length + 1);
            var segments = relative.Split(Path.DirectorySeparatorChar);
            for (int i = 0; i < segments.Length; i++)
            {
                var attributes = AttributesOf(directory);
                if (attributes == null)
                {
                    return new DeleteOutcome(DeleteStatus.Absent);
                }
                if ((attributes.Value & FileAttributes.ReparsePoint) != 0)
                {
                    return DeleteOutcome.Keep(KeepReason.ReparsePoint);
                }
                directory = directory + Path.DirectorySeparatorChar + segments[i];
            }
            var leaf = AttributesOf(key);
            if (leaf == null)
            {
                return new DeleteOutcome(DeleteStatus.Absent);
            }
            if ((leaf.Value & FileAttributes.ReparsePoint) != 0)
            {
                return DeleteOutcome.Keep(KeepReason.ReparsePoint);
            }
            if ((leaf.Value & FileAttributes.Directory) != 0)
            {
                return DeleteOutcome.Keep(KeepReason.Directory);
            }
            // A short alias, with or without '~', may name another file than the long name planned
            if (OperatingSystem.IsWindows() && !string.Equals(LongPathOf(key), LongPathOf(root) + Path.DirectorySeparatorChar + relative, StringComparison.Ordinal))
            {
                return DeleteOutcome.Keep(KeepReason.Alias);
            }
            if (BeforeDeleteForTests is { } hook)
            {
                await hook(target);
            }
            // Deletes one file; a directory substituted since the checks makes it throw instead of recursing
            File.Delete(key);
            return new DeleteOutcome(DeleteStatus.Deleted);
        }

        private string? LongPathOf(string path) => LongPathForTests != null ? LongPathForTests(path) : LongPaths.Of(path);

        // The path's own attributes, a reparse point never followed; null when nothing is there
        private static FileAttributes? AttributesOf(string path)
        {
            try
            {
                return File.GetAttributes(path);
            }
            catch (FileNotFoundException)
            {
                return null;
            }
            catch (DirectoryNotFoundException)
            {
                return null;
            }
        }

        // The table's directory as the store maps it, canonical and without a trailing separator; null when it cannot be formed
        private static string? NativeRootOf(CleanupStoreContract store)
        {
            try
            {
                if (store.PathProblem != null || store.Raw is not ILocalDiskFileStorage local)
                {
                    return null;
                }
                return Path.TrimEndingDirectorySeparator(Path.GetFullPath(local.ToNativeLocalPath(store.Table)));
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Windows long path resolution, which expands every short component of an existing path.
        /// </summary>
        private static class LongPaths
        {
            // The longest path Windows resolves
            private const int MaxLength = 32_767;

            private const string ExtendedPrefix = @"\\?\";

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, EntryPoint = "GetLongPathNameW")]
            [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
            private static extern uint GetLongPathName(string shortPath, [Out] char[] longPath, uint length);

            // In the extended form, which reaches past MAX_PATH; null when the path cannot be resolved
            public static string? Of(string path)
            {
                var extended = Extend(path);
                var buffer = new char[extended.Length + 1];
                while (true)
                {
                    var length = GetLongPathName(extended, buffer, (uint)buffer.Length);
                    if (length == 0)
                    {
                        return null;
                    }
                    if (length < buffer.Length)
                    {
                        return new string(buffer, 0, (int)length);
                    }
                    if (length > MaxLength)
                    {
                        return null;
                    }
                    buffer = new char[length];
                }
            }

            // A canonical drive or UNC path in the extended form
            private static string Extend(string path)
            {
                if (path.StartsWith(ExtendedPrefix, StringComparison.Ordinal) || path.StartsWith(@"\\.\", StringComparison.Ordinal))
                {
                    return path;
                }
                return path.StartsWith(@"\\", StringComparison.Ordinal) ? ExtendedPrefix + @"UNC\" + path.Substring(2) : ExtendedPrefix + path;
            }
        }
    }
}
