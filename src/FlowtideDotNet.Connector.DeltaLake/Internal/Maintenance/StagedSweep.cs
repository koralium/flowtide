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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Stowage;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// What the staged sweep concluded for one listed stage file at or below the published version.
    /// </summary>
    internal enum StageVerdict : byte
    {
        // No rule decides it, so it stays
        Keep = 0,
        // A read failed, stopped or saw a change, so a later turn decides
        Retry = 1,
        // The commit at its version equals it in length and bytes
        Published = 2,
        // A stage with its own prefix whose version holds another commit, and a later commit is listed
        Dead = 3,
        // No commit is listed at its version, which is below the oldest reconstructable version
        BelowOldest = 4
    }

    /// <summary>
    /// What one staged-sweep turn concluded and deleted.
    /// </summary>
    internal sealed class StagedSweepReport
    {
        // By stage file name, for diagnostics and tests
        public Dictionary<string, StageVerdict> Verdicts { get; } = new Dictionary<string, StageVerdict>(StringComparer.Ordinal);

        // Stage files and temporaries
        public long LogFilesDeleted { get; set; }

        // Planned stage files and temporaries that cannot be targets, or that the executor kept
        public long LogFilesKept { get; set; }

        public long PayloadDeleted { get; set; }

        // Owned names that cannot be targets, or that the executor kept
        public long PayloadKept { get; set; }

        // Owned names left behind: their deletes failed in every retry, or the run ended before deleting them
        public long PayloadLeaked { get; set; }

        // Dead stages whose output is left behind for any reason but a delete failing in every retry: unproven, unprotected or unconfirmed, or a run that ended first
        public int LeakedStages { get; set; }

        public MaintenanceReason LeakReason { get; set; }
    }

    /// <summary>
    /// Per-sink counts of what staged sweeps left behind.
    /// </summary>
    internal sealed class SweepCounters
    {
        private long _payloadLeaked;
        private long _leakedStages;

        public long PayloadLeaked => Interlocked.Read(ref _payloadLeaked);

        public long LeakedStages => Interlocked.Read(ref _leakedStages);

        public void Add(StagedSweepReport report)
        {
            Interlocked.Add(ref _payloadLeaked, report.PayloadLeaked);
            Interlocked.Add(ref _leakedStages, report.LeakedStages);
        }
    }

    /// <summary>
    /// One table's staged-commit cleanup: stage files a checked listing proves published, dead or below the oldest reconstructable version, the unreferenced output of dead stages, and old checkpoint temporaries.
    /// </summary>
    internal sealed class StagedSweep
    {
        public const int MaxOwnershipBytes = 4 << 20;
        public const int MaxOwnedFiles = 16_384;

        // Consecutive slices of one run that may conclude nothing before the turn gives up
        private const int MaxGateAttempts = 3;

        // {version:D20}.json between the leading dot and the dot before the id
        private const int StageIdStart = 27;
        private const int StageSuffixLength = 4;

        private static readonly TimeSpan s_minimumTemporaryAge = TimeSpan.FromDays(1);

        private readonly CleanupConfiguration _configuration;
        private readonly IFileStorage _storage;
        private readonly TimeProvider _clock;
        private readonly DeletionRunSettings _settings;

        public StagedSweep(CleanupConfiguration configuration, IFileStorage storage, TimeProvider clock, DeletionRunSettings settings)
        {
            _configuration = configuration;
            _storage = storage;
            _clock = clock;
            _settings = RunSettings(configuration.Store, settings);
        }

        public StagedSweepReport? LastReport { get; private set; }

        // Bounds on a stage's ownership header, lowered by tests
        internal int OwnershipByteLimit { get; init; } = MaxOwnershipBytes;

        internal int OwnedFileLimit { get; init; } = MaxOwnedFiles;

        // Runs before every delete the sweep submits
        internal Func<DeletionTarget, Task>? BeforeDeleteForTests { get; set; }

        // Sees every delete the sweep settles and every target its runs keep before submission
        internal Action<DeletionTarget, DeleteOutcome>? Settled { get; init; }

        /// <summary>
        /// The settings of every run the sweep makes: one outstanding delete on pinned S3, whose signer is not safe for concurrent requests.
        /// </summary>
        internal static DeletionRunSettings RunSettings(CleanupStoreContract store, DeletionRunSettings settings)
        {
            return store.Store.Kind == StoreKind.S3 ? settings with { MaxOutstanding = 1 } : settings;
        }

        /// <summary>
        /// One staged-sweep turn under a delete-class admission; never throws except an OperationCanceledException from the context's stop.
        /// </summary>
        public async Task<CleanupResult> RunAsync(CleanupContext context)
        {
            var turn = new Turn();
            try
            {
                return await RunTurnAsync(context, turn);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (CleanupPreemptedException e) when (context.Stop.IsCancellationRequested)
            {
                throw new OperationCanceledException(e.Message, e, context.Stop);
            }
            catch (CleanupPreemptedException e)
            {
                return new CleanupResult(OutcomeKind.Preempted, e.Stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, e.Message);
            }
            catch (CheckpointDeclinedException e)
            {
                return new CleanupResult(OutcomeKind.Declined, e.Reason, e.Message);
            }
            catch (Exception e)
            {
                return new CleanupResult(OutcomeKind.Failed, MaintenanceReason.Transient, $"{e.GetType().Name}: {e.Message}");
            }
            finally
            {
                LastReport = turn.Report;
                turn.Dispose();
            }
        }

        private async Task<CleanupResult> RunTurnAsync(CleanupContext context, Turn turn)
        {
            // V, captured at admission before any listing
            var published = context.Head;
            if (published < 0)
            {
                return new CleanupResult(OutcomeKind.NoWork);
            }
            if (DeletesDisabled())
            {
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeletesDisabled, "Maintenance deletes are switched off");
            }
            // Every request of the sweep is charged to the maintenance meter
            if (MeteredFileStorage.Of(_storage) == null)
            {
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.Unexpected, "The staged sweep needs the maintenance meter");
            }
            var (check, view) = await ListAsync(published, context);
            if (check != ListingCheck.Passed)
            {
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.ListingIneligible, $"The staged sweep's listing failed its {check} check");
            }

            // A stage above V may still be published by the running sink
            var stages = view!.Stages.Where(x => x.Version <= published).ToList();
            if (2L * stages.Count(x => view.Commit(x.Version) != null) > context.ReadLimit)
            {
                return CleanupResult.Promote;
            }
            var identity = PathCanonicalizer.Create(_configuration.Store, _storage);
            var oldest = view.Oldest;
            foreach (var stage in stages)
            {
                ThrowIfStopped(context);
                var target = view.Commit(stage.Version);
                if (target == null)
                {
                    stage.Verdict = oldest is { } floor && stage.Version < floor ? StageVerdict.BelowOldest : StageVerdict.Keep;
                    continue;
                }
                stage.Target = target.IOEntry;
                await ClassifyAsync(stage, view, identity, context, turn);
            }

            PayloadPlan? payload = null;
            if (view.Log is { } log)
            {
                payload = await PlanPayloadAsync(view, log, context, turn);
            }
            foreach (var stage in stages)
            {
                turn.Report.Verdicts[stage.Entry.Name] = stage.Verdict;
            }

            var temporaryAge = _configuration.OptIns.Retention ?? s_minimumTemporaryAge;
            var planned = stages.Where(x => x.Verdict is StageVerdict.Published or StageVerdict.Dead or StageVerdict.BelowOldest)
                .Concat(view.Temporaries.Where(x => Aged(x.Entry, view.Reference, temporaryAge)))
                .ToList();
            if (planned.Count > 0)
            {
                await RemoveLogFilesAsync(planned, view.Absence, temporaryAge, context, turn);
            }
            if (payload != null)
            {
                await RemovePayloadAsync(payload, context, turn);
            }
            return turn.Result();
        }

        // A checked listing: absence evidence on an eligible store, otherwise positive stage evidence only
        private async Task<(ListingCheck Check, SweepListing? View)> ListAsync(long published, CleanupContext context)
        {
            if (_configuration.Store.Eligible)
            {
                var (check, log) = await ListingEvidence.ReadLogAsync(_configuration, _storage, published, context.Stop, context.Memory);
                return (check, log == null ? null : new SweepListing(log, _storage, _configuration.Store.Table));
            }
            var (stageCheck, stages) = await ListingEvidence.ReadStagesAsync(_configuration, _storage, published, context.Stop, context.Memory);
            return (stageCheck, stages == null ? null : new SweepListing(stages));
        }

        // The writer's publication rules applied to the stage and the commit at its version
        private async Task ClassifyAsync(Planned stage, SweepListing view, PathCanonicalizer identity, CleanupContext context, Turn turn)
        {
            var prefix = DeltaTransactionWriter.StagePrefix(stage.Id);
            // Ownership is only spooled where a dead stage's output may be deleted
            var ownership = view.Absence ? new Ownership(identity, turn.Own(new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Candidate)), OwnershipByteLimit, OwnedFileLimit) : null;
            Comparison compared;
            try
            {
                compared = await CompareAsync(stage, prefix, ownership, context);
                await identity.ChargeOwedAsync(context.Stop);
            }
            catch (Exception e) when (e is not (CleanupPreemptedException or CheckpointDeclinedException) && !context.Stop.IsCancellationRequested)
            {
                ownership?.Set.Dispose();
                stage.Verdict = StageVerdict.Retry;
                turn.Note(MaintenanceReason.Transient, $"A stage could not be read: {e.GetType().Name}");
                return;
            }
            if (!compared.Complete)
            {
                ownership?.Set.Dispose();
                stage.Verdict = StageVerdict.Retry;
                turn.Note(MaintenanceReason.Transient, "A stage or the commit at its version changed after the listing");
                return;
            }
            if (compared.Equal)
            {
                stage.Verdict = StageVerdict.Published;
            }
            else if (compared.TargetHasPrefix || compared.TargetIsPrefix)
            {
                // A repairable or mismatched copy of this stage, which the writer keeps
                stage.Verdict = StageVerdict.Keep;
            }
            else if (!compared.StageHasPrefix)
            {
                // A legacy stage, whose writer takes a missing stage as published
                stage.Verdict = StageVerdict.Keep;
            }
            else
            {
                // The commit after it means the log went on, so the writer can never publish this stage
                stage.Verdict = view.CheckRange(stage.Version, stage.Version + 1) == ListingCheck.Passed ? StageVerdict.Dead : StageVerdict.Keep;
            }
            if (ownership != null && stage.Verdict == StageVerdict.Dead && ownership.Proves(stage.Version))
            {
                ownership.Set.Seal(context.Stop);
                stage.Owned = ownership.Set;
                stage.Unusable = ownership.Unusable;
            }
            else
            {
                ownership?.Set.Dispose();
            }
        }

        private readonly record struct Comparison(bool Complete, bool Equal, bool TargetHasPrefix, bool TargetIsPrefix, bool StageHasPrefix);

        // Both objects read side by side in bounded chunks: their lengths, their common prefix and the stage's own prefix on each; two counted reads
        private async Task<Comparison> CompareAsync(Planned stage, byte[] prefix, Ownership? ownership, CleanupContext context)
        {
            var target = stage.Target!;
            if (!context.TryCountRead() || !context.TryCountRead())
            {
                return default;
            }
            var stageBuffer = Array.Empty<byte>();
            var targetBuffer = Array.Empty<byte>();
            try
            {
                stageBuffer = context.Memory.Allocate(ProofReader.BufferSize);
                targetBuffer = context.Memory.Allocate(ProofReader.BufferSize);
                using var admitted = await context.Transfer.AdmitAsync((stage.Entry.Size ?? 0) + (target.Size ?? 0), context.Stop);
                await using var stageStream = await _storage.OpenRead(stage.Entry.Path, context.Stop);
                await using var targetStream = await _storage.OpenRead(target.Path, context.Stop);
                if (stageStream == null || targetStream == null)
                {
                    return default;
                }
                var s = new Side(stageStream, stageBuffer, prefix, ownership);
                var t = new Side(targetStream, targetBuffer, prefix, null);
                long common = 0;
                var diverged = false;
                while (true)
                {
                    if (s.Available == 0 && !s.Done)
                    {
                        await s.FillAsync(context);
                    }
                    if (t.Available == 0 && !t.Done)
                    {
                        await t.FillAsync(context);
                    }
                    if (s.Available == 0 && s.Done && t.Available == 0 && t.Done)
                    {
                        break;
                    }
                    if (diverged)
                    {
                        // Read on only for the lengths
                        s.Skip(s.Available);
                        t.Skip(t.Available);
                        continue;
                    }
                    var length = Math.Min(s.Available, t.Available);
                    if (length == 0)
                    {
                        // One side ended while the other has bytes
                        diverged = (s.Available == 0 && s.Done) || (t.Available == 0 && t.Done);
                        continue;
                    }
                    var same = s.Chunk(length).CommonPrefixLength(t.Chunk(length));
                    common += same;
                    s.Skip(length);
                    t.Skip(length);
                    diverged = same < length;
                }
                ownership?.End();
                // A length other than the listed size is an object replaced after the listing
                if ((stage.Entry.Size is { } stageSize && stageSize != s.Length) || (target.Size is { } targetSize && targetSize != t.Length))
                {
                    return default;
                }
                return new Comparison(true, s.Length == t.Length && common == s.Length, t.HasPrefix, t.Length < s.Length && common == t.Length, s.HasPrefix);
            }
            finally
            {
                if (stageBuffer.Length > 0)
                {
                    context.Memory.Free(stageBuffer);
                }
                if (targetBuffer.Length > 0)
                {
                    context.Memory.Free(targetBuffer);
                }
            }
        }

        // One object's bytes in a bounded buffer, with its length and whether it starts with the prefix
        private sealed class Side
        {
            private readonly Stream _stream;
            private readonly byte[] _buffer;
            private readonly byte[] _prefix;
            private readonly Ownership? _ownership;
            private int _start;
            private int _end;
            private bool _prefixMatches = true;

            public Side(Stream stream, byte[] buffer, byte[] prefix, Ownership? ownership)
            {
                _stream = stream;
                _buffer = buffer;
                _prefix = prefix;
                _ownership = ownership;
            }

            public long Length { get; private set; }

            public bool Done { get; private set; }

            public int Available => _end - _start;

            public bool HasPrefix => _prefixMatches && Length >= _prefix.Length;

            public ReadOnlySpan<byte> Chunk(int length) => _buffer.AsSpan(_start, length);

            public void Skip(int length) => _start += length;

            public async ValueTask FillAsync(CleanupContext context)
            {
                ThrowIfStopped(context);
                var read = await _stream.ReadAsync(_buffer.AsMemory(), context.Stop);
                _start = 0;
                _end = read;
                if (read == 0)
                {
                    Done = true;
                    return;
                }
                Observe(_buffer.AsSpan(0, read));
            }

            private void Observe(ReadOnlySpan<byte> bytes)
            {
                if (Length < _prefix.Length)
                {
                    var count = (int)Math.Min(bytes.Length, _prefix.Length - Length);
                    _prefixMatches &= bytes.Slice(0, count).SequenceEqual(_prefix.AsSpan((int)Length, count));
                }
                _ownership?.Feed(bytes);
                Length += bytes.Length;
            }
        }

        // A stage's first line through the strict header scanner, its created names spooled into a candidate path set
        private sealed class Ownership
        {
            private readonly CommitHeaderScanner _scanner;
            private readonly CommitReferenceExtractor _extractor;
            private readonly int _byteLimit;
            private readonly int _fileLimit;
            private long _bytes;
            private long _names;
            private bool _ended;

            public Ownership(PathCanonicalizer identity, PathSet set, int byteLimit, int fileLimit)
            {
                Set = set;
                _byteLimit = byteLimit;
                _fileLimit = fileLimit;
                _extractor = new CommitReferenceExtractor(identity, set, PathRole.Candidate);
                _scanner = new CommitHeaderScanner(HeaderMode.Strict, OnCreated);
            }

            public PathSet Set { get; }

            public CommitHeader? Header { get; private set; }

            // Past the byte or name limit, or a name the ledger refused
            public bool OverBounds { get; private set; }

            // Owned names that have no target
            public long Unusable => _extractor.Kept;

            public void Feed(ReadOnlySpan<byte> bytes)
            {
                if (_ended)
                {
                    return;
                }
                var end = bytes.IndexOfAny((byte)'\r', (byte)'\n');
                var line = end < 0 ? bytes : bytes.Slice(0, end);
                if (_bytes + line.Length > _byteLimit)
                {
                    OverBounds = true;
                    End();
                    return;
                }
                _bytes += line.Length;
                _scanner.Feed(line);
                if (end >= 0)
                {
                    Header = _scanner.Complete(terminated: true);
                    _ended = true;
                }
            }

            // The stage ended before its first line did
            public void End()
            {
                if (!_ended)
                {
                    Header = _scanner.Complete(terminated: false);
                    _ended = true;
                }
            }

            /// <summary>
            /// Whether the first line strictly proves an own attempt's complete list of created files.
            /// </summary>
            public bool Proves(long version)
            {
                return !OverBounds && Header is { HasCreatedFiles: true } header && CommitProvenance.FromStrict(version, header).IsOwn;
            }

            private void OnCreated(string name)
            {
                if (OverBounds)
                {
                    return;
                }
                if (++_names > _fileLimit)
                {
                    OverBounds = true;
                    return;
                }
                try
                {
                    _extractor.AddName(name);
                }
                catch (CheckpointDeclinedException)
                {
                    OverBounds = true;
                }
            }
        }

        private sealed record PayloadPlan(IReadOnlyList<Planned> Stages, PathSet Protection, CleanupEvidence Evidence, long From);

        // Dead stages with proven ownership, an own head at the planning listing, and protection from every commit since the oldest of them and every other stage
        private async Task<PayloadPlan?> PlanPayloadAsync(SweepListing view, LogEvidence log, CleanupContext context, Turn turn)
        {
            var dead = view.Stages.Where(x => x.Verdict == StageVerdict.Dead).ToList();
            var unproven = dead.Count(x => x.Owned == null);
            if (unproven > 0)
            {
                // Legacy or unreadable ownership: the stage alone is removed and its output stays behind
                turn.Leak(unproven, MaintenanceReason.CreatedFilesUnreadable, $"{unproven} dead stages do not prove what they created");
            }
            // Owned names that are no target stay behind with or without the rest
            turn.Report.PayloadKept += dead.Where(x => x.Owned is { Count: 0 }).Sum(x => x.Unusable);
            var owned = dead.Where(x => x.Owned is { Count: > 0 }).ToList();
            if (owned.Count == 0)
            {
                return null;
            }
            var from = owned.Min(x => x.Version);
            var head = log.Head;
            if (log.CheckRange(from, head) != ListingCheck.Passed)
            {
                turn.Leak(owned.Count, MaintenanceReason.MissingHistory, $"Commits from {from} to {head} are not all listed");
                return null;
            }
            var established = await CleanupEvidence.EstablishAsync(_configuration, _storage, log, head, null, durable: false, context);
            if (!established.Passed)
            {
                if (established.Action == GateAction.Retry)
                {
                    turn.Defer(owned, established.Reason, $"Dead stage output waits: {established.Verdict}");
                }
                else
                {
                    // Without an own head nothing proves the output unreferenced
                    turn.Leak(owned.Count, established.Reason == MaintenanceReason.None ? MaintenanceReason.CleanupGate : established.Reason, $"Dead stage output is kept: {established.Verdict}");
                }
                return null;
            }
            var others = view.Stages.Where(x => x.Verdict != StageVerdict.Dead).ToList();
            // Each payload slice's gate rehashes the anchor and the capability sources, and reads a moved head
            var slices = (owned.Sum(x => x.Owned!.Count) + Math.Max(1, _settings.MaxPaths) - 1) / Math.Max(1, _settings.MaxPaths);
            var reads = head - from + 1 + others.Count + slices * (2 + established.Evidence!.Capability.Sources.Count());
            var budget = Math.Min(context.ReadsRemaining, MaintenanceWorkerSettings.DefaultShortReads - context.ReadsUsed);
            if (reads > budget)
            {
                turn.Leak(owned.Count, MaintenanceReason.EntriesOverCap, $"Protecting dead stage output needs {reads} reads and the short budget has {budget}");
                return null;
            }

            var identity = PathCanonicalizer.Create(_configuration.Store, _storage);
            var protection = turn.Own(new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Protection));
            var extractor = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
            var objects = Enumerable.Range(0, (int)(head - from + 1)).Select(x => log.Listing.Commits[from + x].IOEntry).Concat(others.Select(x => x.Entry));
            foreach (var entry in objects)
            {
                var read = await AddReferencesAsync(entry, extractor, identity, context);
                if (read != ReferenceRead.Read)
                {
                    turn.Defer(owned, read == ReferenceRead.Declined ? MaintenanceReason.PathRejected : MaintenanceReason.Transient,
                        read == ReferenceRead.Declined ? $"A protecting reference is unusable: {extractor.DeclineDetail}" : "A protecting object changed or could not be read");
                    return null;
                }
            }
            protection.Seal(context.Stop);
            return new PayloadPlan(owned, protection, established.Evidence!, from);
        }

        private enum ReferenceRead : byte
        {
            Read,
            // Gone, not its listed size, unreadable, or the reads were spent
            Changed,
            // A line or path protection cannot use
            Declined
        }

        // Every path one commit or stage references, added to the protection set; one counted read
        private async Task<ReferenceRead> AddReferencesAsync(IOEntry entry, CommitReferenceExtractor extractor, PathCanonicalizer identity, CleanupContext context)
        {
            if (!context.TryCountRead())
            {
                return ReferenceRead.Changed;
            }
            try
            {
                using var admitted = await context.Transfer.AdmitAsync(entry.Size ?? 0, context.Stop);
                var source = await _storage.OpenRead(entry.Path, context.Stop);
                if (source == null)
                {
                    return ReferenceRead.Changed;
                }
                var proof = new ProofStream(source, context, proveFirstLine: false);
                BoundedLineReader lines;
                try
                {
                    lines = new BoundedLineReader(proof, context.Memory, BoundedCheckpointReader.ActionCap, skipOversizedCommitInfo: true, ownsStream: true);
                }
                catch
                {
                    await proof.DisposeAsync();
                    throw;
                }
                using (lines)
                {
                    while (await lines.ReadLineAsync(context.Stop))
                    {
                        if (!extractor.AddLine(lines.Line.Span))
                        {
                            return ReferenceRead.Declined;
                        }
                    }
                }
                await identity.ChargeOwedAsync(context.Stop);
                return entry.Size is { } listed && listed != proof.BytesRead ? ReferenceRead.Changed : ReferenceRead.Read;
            }
            catch (CheckpointDeclinedException)
            {
                return ReferenceRead.Declined;
            }
            catch (Exception e) when (e is not CleanupPreemptedException && !context.Stop.IsCancellationRequested)
            {
                return ReferenceRead.Changed;
            }
        }

        // Stages and temporaries through one volatile run whose every slice relists and finds each remaining target as it was planned
        private async Task RemoveLogFilesAsync(List<Planned> planned, bool absence, TimeSpan temporaryAge, CleanupContext context, Turn turn)
        {
            var executor = NewExecutor();
            var targets = new List<DeletionTarget>();
            var issued = new List<Planned>();
            long rejected = 0;
            foreach (var item in planned)
            {
                if (executor.TryTarget(DeltaTransactionReader.DeltaLogDirName + item.Entry.Name, item.Kind, out var target) == KeepReason.None)
                {
                    targets.Add(target);
                    issued.Add(item);
                }
                else
                {
                    rejected++;
                }
            }
            DeletionRun? run = null;
            var gate = absence
                ? ChunkGate.ForLog(_configuration, _storage, (log, _) => Task.FromResult(Recheck(new SweepListing(log, _storage, _configuration.Store.Table), issued, run!, temporaryAge)))
                : ChunkGate.ForStages(_configuration, _storage, (stages, _) => Task.FromResult(Recheck(new SweepListing(stages), issued, run!, temporaryAge)));
            run = new DeletionRun(executor, gate, new ListDeletionSource(targets), _clock, _settings);
            var slice = await DrainAsync(run, context);
            turn.Report.LogFilesDeleted += executor.Tally.Deleted;
            turn.Report.LogFilesKept += rejected + executor.Tally.Kept;
            turn.NoteSlice(slice, "stage and temporary");
            turn.NoteFailures(executor.Tally);
        }

        // Every target the run has not passed is listed as it was planned and its evidence still holds; one an earlier slice reached may be gone
        private static GateResult Recheck(SweepListing listing, IReadOnlyList<Planned> planned, DeletionRun run, TimeSpan temporaryAge)
        {
            var oldest = listing.Oldest;
            for (var i = (int)run.Cursor; i < planned.Count; i++)
            {
                var item = planned[i];
                var current = listing.Hidden(item.Entry.Path.Full);
                if (current == null && i < run.Reached)
                {
                    continue;
                }
                if (current == null || !SameListed(current, item.Entry))
                {
                    return new GateResult(GateVerdict.Changed, $"A planned {item.Kind} file is gone or listed differently");
                }
                var holds = item.Kind == TargetKind.Temporary ? Aged(current, listing.Reference, temporaryAge) : item.Verdict switch
                {
                    StageVerdict.BelowOldest => listing.Commit(item.Version) == null && oldest is { } floor && item.Version < floor,
                    _ => listing.Commit(item.Version) is { } commit && SameListed(commit.IOEntry, item.Target!)
                        && (item.Verdict != StageVerdict.Dead || listing.CheckRange(item.Version, item.Version + 1) == ListingCheck.Passed)
                };
                if (!holds)
                {
                    return new GateResult(GateVerdict.Changed, $"The evidence for a planned {item.Kind} file changed");
                }
            }
            return new GateResult(GateVerdict.Passed);
        }

        // The dead stages are confirmed gone before any of their output is deleted, so no writer can publish them from their files afterwards
        private async Task RemovePayloadAsync(PayloadPlan payload, CleanupContext context, Turn turn)
        {
            var confirmed = new List<PathSet>();
            var unconfirmed = 0;
            long unusable = 0;
            foreach (var stage in payload.Stages)
            {
                if (context.CheckStop() != IssuanceStop.None)
                {
                    unconfirmed++;
                    continue;
                }
                IOEntry? entry;
                try
                {
                    entry = await _storage.Stat(stage.Entry.Path, context.Stop);
                }
                catch (Exception) when (!context.Stop.IsCancellationRequested)
                {
                    unconfirmed++;
                    continue;
                }
                // A stage still listed keeps its ownership for a later turn
                if (entry == null)
                {
                    confirmed.Add(stage.Owned!);
                    unusable += stage.Unusable;
                }
            }
            if (unconfirmed > 0)
            {
                turn.Leak(unconfirmed, MaintenanceReason.Transient, $"{unconfirmed} dead stages could not be confirmed removed");
            }
            if (confirmed.Count == 0)
            {
                return;
            }
            var executor = NewExecutor();
            var source = new PayloadSource(confirmed, payload.Protection, executor);
            var run = new DeletionRun(executor, ChunkGate.ForEvidence(_configuration, _storage, payload.Evidence, payload.From), source, _clock, _settings);
            var slice = await DrainAsync(run, context);
            turn.Report.PayloadDeleted += executor.Tally.Deleted;
            turn.Report.PayloadKept += unusable + executor.Tally.Kept;
            turn.Report.PayloadLeaked += executor.Tally.Leaked;
            turn.NoteFailures(executor.Tally);
            if (slice.Status != SliceStatus.Completed)
            {
                // The run is volatile, so what it did not reach stays behind
                var reason = slice.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : slice.Reason;
                var detail = $"Dead stage output stopped at {run.Cursor}: {slice.Status}";
                if (source.TryCountLeft(run, context.Stop, out var files, out var stages))
                {
                    turn.Report.PayloadLeaked += files;
                    turn.Leak(stages, reason, detail);
                }
                else
                {
                    // What is left is unknown, so every confirmed stage counts
                    turn.Leak(confirmed.Count, reason, detail);
                }
            }
        }

        /// <summary>
        /// The executor's own form of a candidate's exact full path; any other path is handed on as given, for the run's check to keep.
        /// </summary>
        internal static DeletionTarget PayloadTarget(DeletionExecutor executor, string path) => executor.TargetOf(path);

        // The confirmed dead stages' owned names no protection key holds, read again from the cursor for each slice
        private sealed class PayloadSource : IDeletionSource
        {
            private readonly IReadOnlyList<PathSet> _owned;
            private readonly PathSet _protection;
            private readonly DeletionExecutor _executor;

            public PayloadSource(IReadOnlyList<PathSet> owned, PathSet protection, DeletionExecutor executor)
            {
                _owned = owned;
                _protection = protection;
                _executor = executor;
            }

            public async IAsyncEnumerable<DeletionTarget> ReadFrom(long position, [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                await Task.CompletedTask;
                long index = 0;
                foreach (var owned in _owned)
                {
                    using var difference = PathSet.Difference(owned, _protection);
                    while (difference.MoveNext())
                    {
                        if (index++ < position)
                        {
                            continue;
                        }
                        cancellationToken.ThrowIfCancellationRequested();
                        yield return PayloadTarget(_executor, difference.DeletionPathText);
                    }
                }
            }

            /// <summary>
            /// The names the run left undeleted from its cursor on and the stages that own any of them, read from scratch; false when the sets cannot be read or the worker stops.
            /// </summary>
            public bool TryCountLeft(DeletionRun run, CancellationToken stop, out long files, out int stages)
            {
                files = 0;
                stages = 0;
                try
                {
                    long index = 0;
                    foreach (var owned in _owned)
                    {
                        stop.ThrowIfCancellationRequested();
                        long left = 0;
                        using var difference = PathSet.Difference(owned, _protection);
                        while (difference.MoveNext())
                        {
                            stop.ThrowIfCancellationRequested();
                            var position = index++;
                            if (position >= run.Cursor && !run.DoneAhead(position))
                            {
                                left++;
                            }
                        }
                        files += left;
                        stages += left > 0 ? 1 : 0;
                    }
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        }

        // Slices until the run ends or issuance stops; a failed delete is retried after a backoff, and a gate that concludes nothing several times in a row ends the run
        private static async Task<SliceResult> DrainAsync(DeletionRun run, CleanupContext context)
        {
            var attempts = 0;
            var failures = 0;
            while (true)
            {
                var cursor = run.Cursor;
                var slice = await run.RunSliceAsync(context);
                var open = context.CheckStop() == IssuanceStop.None;
                var moved = run.Cursor > cursor;
                if (slice.Status == SliceStatus.Failed)
                {
                    // Failed slices in a row at one cursor; the run passes a volatile target at its leak limit
                    failures = moved ? 1 : failures + 1;
                    if (open && await run.BackOffAsync(failures, context))
                    {
                        attempts = 0;
                        continue;
                    }
                    return slice;
                }
                failures = 0;
                if (slice.Status == SliceStatus.Progressed && moved && open)
                {
                    attempts = 0;
                    continue;
                }
                if (slice.Status is SliceStatus.Retry or SliceStatus.Progressed && ++attempts < MaxGateAttempts && open)
                {
                    continue;
                }
                return slice;
            }
        }

        private DeletionExecutor NewExecutor() => new DeletionExecutor(_configuration, _storage) { BeforeDeleteForTests = BeforeDeleteForTests, Settled = Settled, Withheld = Settled };

        // A switch that cannot be read counts as on
        private bool DeletesDisabled()
        {
            try
            {
                return _settings.DeletesDisabled?.Invoke() == true;
            }
            catch
            {
                return true;
            }
        }

        private static void ThrowIfStopped(CleanupContext context)
        {
            if (context.CheckStop() is var stop && stop != IssuanceStop.None)
            {
                throw new CleanupPreemptedException(stop);
            }
        }

        // The listed size, modification time and MD5 are the ones planning saw
        private static bool SameListed(IOEntry current, IOEntry planned)
        {
            return current.Size == planned.Size && current.LastModificationTime == planned.LastModificationTime && current.MD5 == planned.MD5;
        }

        // At least the age older than the newest listed commit; a missing time never ages
        private static bool Aged(IOEntry entry, DateTimeOffset? reference, TimeSpan age)
        {
            return reference is { } newest && entry.LastModificationTime is { } modified && newest - modified >= age;
        }

        // A log-side file the turn may remove, with the evidence it was planned on
        private sealed class Planned
        {
            private Planned(IOEntry entry, TargetKind kind, long version)
            {
                Entry = entry;
                Kind = kind;
                Version = version;
            }

            public static Planned Stage(IOEntry entry, long version) => new Planned(entry, TargetKind.Stage, version);

            public static Planned Temporary(IOEntry entry) => new Planned(entry, TargetKind.Temporary, -1);

            public IOEntry Entry { get; }

            public TargetKind Kind { get; }

            public long Version { get; }

            // The id a stage is named by: .{version:D20}.json.{id}.tmp
            public string Id => Entry.Name.Substring(StageIdStart, Entry.Name.Length - StageIdStart - StageSuffixLength);

            public StageVerdict Verdict { get; set; }

            // The commit at the stage's version as planning listed it
            public IOEntry? Target { get; set; }

            // A dead stage's sealed candidates when its ownership is proven
            public PathSet? Owned { get; set; }

            public long Unusable { get; set; }
        }

        // One checked listing as the sweep reads it: absence evidence on an eligible store, positive stage evidence otherwise
        private sealed class SweepListing
        {
            private readonly StageEvidence? _stages;
            private readonly Dictionary<string, IOEntry> _hidden = new Dictionary<string, IOEntry>(StringComparer.Ordinal);

            public SweepListing(LogEvidence log, IFileStorage storage, IOPath table)
            {
                Log = log;
                var entries = log.Entries;
                for (int i = 0; i < entries.Count; i++)
                {
                    // A folder's name ends in its slash, so it is never taken for either
                    var entry = entries[i];
                    if (ListingEvidence.TryGetStageVersion(entry.Name, out var version))
                    {
                        Stages.Add(Planned.Stage(entry, version));
                        _hidden[entry.Path.Full] = entry;
                    }
                    else if (DeletionExecutor.IsTemporary(entry.Name))
                    {
                        Temporaries.Add(Planned.Temporary(entry));
                        _hidden[entry.Path.Full] = entry;
                    }
                }
                Oldest = OldestOf(log, storage, table);
                Reference = log.Listing.Commits.Values.Max(x => x.IOEntry.LastModificationTime);
            }

            public SweepListing(StageEvidence stages)
            {
                _stages = stages;
                foreach (var stage in stages.Stages)
                {
                    Stages.Add(Planned.Stage(stage.Stage, stage.Version));
                    _hidden[stage.Stage.Path.Full] = stage.Stage;
                }
            }

            public LogEvidence? Log { get; }

            public bool Absence => Log != null;

            public List<Planned> Stages { get; } = new List<Planned>();

            // Only with absence evidence
            public List<Planned> Temporaries { get; } = new List<Planned>();

            // The oldest listed root the contiguous commits reach to the head; only with absence evidence
            public long? Oldest { get; }

            // The newest listed commit's modification time; only with absence evidence
            public DateTimeOffset? Reference { get; }

            public LogTransactionFile? Commit(long version) => Log != null ? (Log.Listing.Commits.TryGetValue(version, out var commit) ? commit : null) : _stages!.ListedCommit(version);

            public ListingCheck CheckRange(long from, long to) => Log != null ? Log.CheckRange(from, to) : _stages!.CheckRange(from, to);

            public IOEntry? Hidden(string full) => _hidden.TryGetValue(full, out var entry) ? entry : null;

            private static long? OldestOf(LogEvidence log, IFileStorage storage, IOPath table)
            {
                var segment = new DeltaLogSegment(storage, table, log.Listing);
                long? oldest = null;
                // Newest first, so the last is the oldest
                foreach (var root in segment.RootCandidates(log.Head, log.Head))
                {
                    oldest = root.Version;
                }
                return oldest;
            }
        }

        // What one turn holds and reports; its path sets are deleted when it ends
        private sealed class Turn : IDisposable
        {
            private readonly List<PathSet> _sets = new List<PathSet>();
            private (MaintenanceReason Reason, string Detail)? _problem;

            public StagedSweepReport Report { get; } = new StagedSweepReport();

            public PathSet Own(PathSet set)
            {
                _sets.Add(set);
                return set;
            }

            // The first reason the turn did less than it planned
            public void Note(MaintenanceReason reason, string detail)
            {
                _problem ??= (reason, detail);
            }

            public void NoteSlice(SliceResult slice, string what)
            {
                if (slice.Status != SliceStatus.Completed)
                {
                    Note(slice.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : slice.Reason, $"The {what} run ended {slice.Status}");
                }
            }

            // Deletes a run left behind after their retries
            public void NoteFailures(DeletionTally tally)
            {
                if (tally.Leaked > 0)
                {
                    Note(MaintenanceReason.DeleteFailed, $"{tally.Leaked} deletes failed in every retry");
                }
            }

            // Dead stages whose output is left behind, counted
            public void Leak(int stages, MaintenanceReason reason, string detail)
            {
                if (Report.LeakedStages == 0)
                {
                    Report.LeakReason = reason;
                }
                Report.LeakedStages += stages;
                Note(reason, detail);
            }

            // Dead stages kept with their ownership for a later turn
            public void Defer(IEnumerable<Planned> stages, MaintenanceReason reason, string detail)
            {
                foreach (var stage in stages)
                {
                    stage.Verdict = StageVerdict.Retry;
                    stage.Owned?.Dispose();
                    stage.Owned = null;
                }
                Note(reason == MaintenanceReason.None ? MaintenanceReason.Transient : reason, detail);
            }

            public CleanupResult Result()
            {
                if (_problem is { } problem)
                {
                    return new CleanupResult(OutcomeKind.Declined, problem.Reason, problem.Detail);
                }
                return Report.LogFilesDeleted + Report.PayloadDeleted > 0 ? new CleanupResult(OutcomeKind.Completed) : new CleanupResult(OutcomeKind.NoWork);
            }

            public void Dispose()
            {
                foreach (var set in _sets)
                {
                    set.Dispose();
                }
            }
        }
    }
}
