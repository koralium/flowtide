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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using Stowage.Impl;
using System.Security.Cryptography;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    internal enum RootPolicy : byte
    {
        Auto,
        None
    }

    internal enum BuildStatus : byte
    {
        Completed,
        AlreadyValid,
        Declined,
        Failed,
        // The replay needs more commit reads than the request allows; nothing was opened
        NeedsLongAdmission
    }

    internal enum BuildMode : byte
    {
        Probe,
        Sort
    }

    /// <summary>
    /// A listed object's identity: version, listed size, and modification time and MD5 when the listing has them.
    /// </summary>
    internal sealed record ObjectIdentity(string Table, long Version, long Size, DateTimeOffset? Modified, string? Md5)
    {
        public bool Strong => Modified != null || Md5 != null;

        public static ObjectIdentity Of(IOPath table, LogTransactionFile file) =>
            new ObjectIdentity(table.Full, file.Version, file.IOEntry.Size ?? -1, file.IOEntry.LastModificationTime, file.IOEntry.MD5);
    }

    internal sealed class BuildRequest
    {
        public required long Version { get; init; }

        public RootPolicy Root { get; init; } = RootPolicy.Auto;

        public string? TableId { get; init; }

        // Roots this worker found unsupported, skipped while their identity holds
        public IReadOnlySet<ObjectIdentity> KnownUnsupported { get; init; } = new HashSet<ObjectIdentity>();

        // Cached roots with a weak identity apply only above it
        public long WeakFloor { get; init; }

        // Commit reads the replay may make, null for no limit
        public long? ReadLimit { get; init; }

        // On the context clock: no commit is opened and nothing is published after it
        public DateTimeOffset? Deadline { get; init; }
    }

    /// <summary>
    /// A built and locally validated checkpoint in job scratch.
    /// </summary>
    internal sealed record CheckpointDescriptor(long Version, string LocalPath, long SizeInBytes, long Actions, long AddFiles, CoverageMarker Marker, byte[] Sha256);

    internal sealed record BuildResult(BuildStatus Status, CheckpointDescriptor? Checkpoint = null, MaintenanceReason Reason = MaintenanceReason.None,
        DeclineDependency Dependency = DeclineDependency.None, string? Detail = null, ObjectIdentity? RootIdentity = null)
    {
        public BuildMode Mode { get; init; }

        public double ValidationMs { get; init; }

        public long? RootVersion { get; init; }

        public long SpillBytes { get; init; }

        public long TransferBytes { get; init; }

        public long SkippedLines { get; init; }

        // Root candidates skipped as out of the envelope or unsupported, one diagnostic each
        public IReadOnlyList<string> Skipped { get; init; } = Array.Empty<string>();

        // The listed checkpoints at or below the target with contiguous commits to it, as identified by the listing
        public IReadOnlyList<ObjectIdentity> Candidates { get; init; } = Array.Empty<ObjectIdentity>();

        public ObjectIdentity? TargetCommit { get; init; }

        // The commit whose line a tail-dependent decline came from
        public ObjectIdentity? TailCommit { get; init; }

        // The tail commits that hold the effective protocol and metaData; a root's are covered by the candidates
        public IReadOnlyList<ObjectIdentity> ProtocolCommits { get; init; } = Array.Empty<ObjectIdentity>();

        // The newest candidate below the target that was valid, admitted or out of the envelope
        public long? NewestValidRoot { get; init; }

        // Why the legacy rebuild kept a root with a gap, which the worker reports once
        public string? LegacyGapKept { get; init; }

        // Known unsupported roots whose listing identity changed, so their classification no longer applies
        public IReadOnlyList<ObjectIdentity> Stale { get; init; } = Array.Empty<ObjectIdentity>();

        // What the build's listing's control records forbid, none before the listing
        public ControlFence Fence { get; init; } = ControlFence.None;
    }

    internal sealed class CheckpointBuildContext
    {
        public required IFileStorage Storage { get; init; }

        public required IOPath Table { get; init; }

        public required JobScratch Scratch { get; init; }

        private ICheckpointTransport? _transport;

        public ICheckpointTransport Transport
        {
            get => _transport ??= new StowageCheckpointTransport(Storage, Table);
            init => _transport = value;
        }

        // Admits downloads and commits by listed size; none in tests that do not share a budget
        public TransferAllowance? Transfer { get; init; }

        // Local roots; used only when the request knows the table id
        public RootCache? Cache { get; init; }

        // Off in tests that read the output in scratch
        public bool Publish { get; init; } = true;

        public Func<string, long>? FreeSpaceForTests { get; init; }

        // Throws at a named point of publication: move, rename or hint
        public Action<string>? PublishFaultForTests { get; init; }

        // Throws at a named point of the build: run write, merge or writer close
        public Action<string>? BuildFaultForTests { get; init; }

        public required JobMemory Memory { get; init; }

        public TimeProvider Clock { get; init; } = TimeProvider.System;

        public DeltaCompressionCodec? Codec { get; init; }

        public bool RebuildLegacyCheckpoints { get; init; }

        // Cleanup-eligible storage; off, a torn rebuild also needs no control fence and every commit from zero
        public bool CleanupEligible { get; init; }

        public long ArenaCap { get; init; } = TailReducer.DefaultArenaCap;

        public int LineCap { get; init; } = BoundedCheckpointReader.ActionCap;

        // Commit opens a replay keeps ahead of its reader; 1 reads one at a time
        public int ReadAhead { get; init; } = 1;

        // Runs after the output is written and before it is validated, so tests can damage it
        public Action<string>? BeforeValidationForTests { get; init; }
    }

    /// <summary>
    /// Builds the classic checkpoint at a version into job scratch and validates it there.
    /// </summary>
    internal sealed class CheckpointBuilder
    {
        private readonly CheckpointBuildContext _context;
        private readonly BuildRequest _request;
        private readonly List<string> _skipped = new List<string>();
        private readonly List<ObjectIdentity> _stale = new List<ObjectIdentity>();
        private IReadOnlyList<ObjectIdentity> _candidates = Array.Empty<ObjectIdentity>();
        private ObjectIdentity? _targetCommit;
        private ObjectIdentity? _tailCommit;
        private readonly List<ObjectIdentity> _protocolCommits = new List<ObjectIdentity>();
        private long? _newestValidRoot;
        private string? _legacyGapKept;
        private readonly RootCacheScope? _cache;
        private long _transferBytes;
        private int _skippedLines;
        private byte[] _key = Array.Empty<byte>();
        private ControlFence _fence = ControlFence.None;

        private CheckpointBuilder(CheckpointBuildContext context, BuildRequest request)
        {
            _context = context;
            _request = request;
            // Cache keys hold metaData.id, so a build that does not know it neither reads nor fills the cache
            _cache = context.Cache != null && request.TableId != null ? new RootCacheScope(context.Cache, context.Storage, request.TableId, request.WeakFloor) : null;
        }

        public static async Task<BuildResult> BuildAsync(CheckpointBuildContext context, BuildRequest request, CancellationToken cancellationToken = default)
        {
            var builder = new CheckpointBuilder(context, request);
            try
            {
                return await builder.BuildAsync(cancellationToken);
            }
            finally
            {
                builder.FreeKey();
            }
        }

        private BuildResult Result(BuildStatus status, MaintenanceReason reason = MaintenanceReason.None, DeclineDependency dependency = DeclineDependency.None, string? detail = null, ObjectIdentity? root = null)
        {
            return new BuildResult(status, null, reason, dependency, detail, root)
            {
                TransferBytes = _transferBytes,
                Skipped = _skipped,
                SkippedLines = _skippedLines,
                Candidates = _candidates,
                TargetCommit = _targetCommit,
                TailCommit = dependency == DeclineDependency.Tail ? _tailCommit : null,
                ProtocolCommits = dependency == DeclineDependency.Protocol ? _protocolCommits : Array.Empty<ObjectIdentity>(),
                NewestValidRoot = _newestValidRoot,
                LegacyGapKept = _legacyGapKept,
                Stale = _stale,
                Fence = _fence
            };
        }

        private async Task<BuildResult> BuildAsync(CancellationToken cancellationToken)
        {
            var target = _request.Version;
            // One listing; never read or write above the target
            var segment = await _context.Transport.ListAsync(cancellationToken);
            // Its control records decide what this build may change
            _fence = ControlFence.Of(segment.Listing);
            var hint = await _context.Transport.ReadHintAsync(cancellationToken);
            if (target < 0 || target > segment.Head || !segment.Listing.Commits.TryGetValue(target, out var targetFile))
            {
                return Result(BuildStatus.Failed, MaintenanceReason.TargetNotInLog, detail: $"Version {target} is not in the log, whose head is {segment.Head}");
            }
            _candidates = segment.RootCandidates(target, target).Select(x => ObjectIdentity.Of(_context.Table, x)).ToList();
            _targetCommit = ObjectIdentity.Of(_context.Table, targetFile);

            // A checkpoint already listed at the target
            var existing = segment.Listing.Checkpoints.FirstOrDefault(x => x.Version == target);
            if (existing != null)
            {
                // The hint fast path: a hint naming this checkpoint with its listed size is trusted without a download
                var trusted = _request.Root == RootPolicy.Auto && hint != null && hint.Version == target && hint.SizeInBytes != null && hint.SizeInBytes == existing.IOEntry.Size;
                if (!trusted && PastDeadline())
                {
                    return DeadlineResult();
                }
                InspectedCheckpoint inspected;
                try
                {
                    inspected = trusted ? new InspectedCheckpoint(CheckpointClass.Valid, false, hint!.Size, hint.SizeInBytes!.Value, null)
                        : await CheckpointInspector.InspectAsync(_context.Transport, _context.Scratch, _context.Memory, _context.Transfer, existing, cancellationToken, _cache);
                }
                catch (CheckpointDeclinedException e)
                {
                    return Result(BuildStatus.Declined, e.Reason, e.Dependency, e.Message);
                }
                if (!trusted && inspected.Downloaded)
                {
                    _transferBytes += inspected.Length;
                }
                switch (inspected.Class)
                {
                    case CheckpointClass.Unknown:
                        return Result(BuildStatus.Failed, MaintenanceReason.UnknownCheckpoint, detail: inspected.Detail);
                    case CheckpointClass.Valid when _request.Root == RootPolicy.None:
                    case CheckpointClass.Unsupported when _request.Root == RootPolicy.None:
                        return Result(BuildStatus.Declined, MaintenanceReason.CheckpointExists, detail: $"A checkpoint at {target} exists");
                    case CheckpointClass.Valid:
                        // Inspection knows the size it read and the actions from the footer, not the add count
                        var size = inspected.Length;
                        if (_context.Publish && CheckpointInspector.HintNeedsWrite(hint, target, inspected.Rows, size))
                        {
                            if (!_fence.AllowsHint(target))
                            {
                                // The checkpoint stands; only its hint is kept
                                return Result(BuildStatus.AlreadyValid, MaintenanceReason.CleanupGate, detail: $"The hint stays below checkpoint {target}: {_fence.Detail}");
                            }
                            if (PastDeadline())
                            {
                                return DeadlineResult();
                            }
                            await _context.Transport.WriteHintAsync(new LastCheckpointInfo() { Version = target, Size = inspected.Rows, SizeInBytes = size }, cancellationToken);
                        }
                        return Result(BuildStatus.AlreadyValid);
                    case CheckpointClass.Unsupported:
                        return Result(BuildStatus.Completed);
                }
                // Torn: rebuilt in place, unless a control record or an ineligible store's listing defers it
                var head = Math.Max(segment.Head, segment.Listing.MaxOtherVersion);
                if (!_fence.AllowsTornRepair(target, _context.CleanupEligible, segment.HasHistoryFromZero(head)))
                {
                    var why = _context.CleanupEligible || !_fence.AllowsReplace(target) ? _fence.Detail
                        : _fence.Clear ? $"on a store not eligible for cleanup, commits before {segment.SuffixStart(head)} are missing" : $"on a store not eligible for cleanup, {_fence.Detail}";
                    return Result(BuildStatus.Declined, MaintenanceReason.CleanupGate, detail: $"The torn checkpoint at {target} is not rebuilt: {why}");
                }
            }
            else if (!_fence.AllowsCreate(target))
            {
                return Result(BuildStatus.Declined, MaintenanceReason.CleanupGate, detail: $"No checkpoint is created at {target}: {_fence.Detail}");
            }

            // The root
            var policy = _request.Root;
            // Even the newest root leaves too many commits, so the job is promoted before any root is fetched
            var newestRoot = policy == RootPolicy.Auto ? segment.RootCandidates(target, target - 1).FirstOrDefault()?.Version ?? -1 : -1;
            if (OverReadLimit(target - newestRoot))
            {
                return Promotion(target - newestRoot);
            }
            LocalRoot? root = null;
            try
            {
                CheckDeadline();
                if (policy == RootPolicy.Auto)
                {
                    var (chosen, blocked, failure) = await ChooseRootAsync(segment, target, cancellationToken);
                    if (failure != null)
                    {
                        return failure;
                    }
                    root = chosen;
                    if (root == null)
                    {
                        // A blocked root replays from 0 only with the option and the whole history; missing history is a decline of its own only when nothing is blocked
                        if (blocked && (!_context.RebuildLegacyCheckpoints || !segment.HasHistoryFromZero(target)))
                        {
                            return Result(BuildStatus.Declined, MaintenanceReason.NoAdmissibleRoot, DeclineDependency.Root, string.Join("; ", _skipped));
                        }
                        if (!segment.HasHistoryFromZero(target))
                        {
                            return Result(BuildStatus.Declined, MaintenanceReason.MissingHistory, detail: $"Commits before {segment.SuffixStart(target)} are missing");
                        }
                    }
                    else if (_context.RebuildLegacyCheckpoints && (root.Marker == null || root.Marker.Inherited != null))
                    {
                        if (segment.HasHistoryFromZero(target))
                        {
                            root.Dispose(_context);
                            root = null;
                        }
                        else
                        {
                            _legacyGapKept = $"A checkpoint without full coverage stays the root: commits before {segment.SuffixStart(target)} are missing";
                        }
                    }
                }
                else if (!segment.HasHistoryFromZero(target))
                {
                    return Result(BuildStatus.Declined, MaintenanceReason.MissingHistory, detail: $"Commits before {segment.SuffixStart(target)} are missing");
                }

                // A replay without a root reads target + 1 commits
                if (OverReadLimit(target - (root?.Version ?? -1)))
                {
                    return Promotion(target - (root?.Version ?? -1));
                }
                return await BuildFromAsync(segment, target, root, cancellationToken);
            }
            catch (CheckpointDeclinedException e)
            {
                // A root that fails to decode after a clean scan, or uses sidecars, is unsupported under its identity
                if (root != null && e.Reason is MaintenanceReason.RootDecodeFailure or MaintenanceReason.SidecarRoot)
                {
                    // Bytes known to be bad leave the cache at once
                    if (_cache != null)
                    {
                        _cache.Cache.Drop(_cache, root.Identity);
                    }
                    return Result(BuildStatus.Failed, e.Reason, e.Dependency, e.Message, root.Identity);
                }
                return Result(BuildStatus.Declined, e.Reason, e.Dependency, e.Message, root?.Identity);
            }
            finally
            {
                root?.Dispose(_context);
            }
        }

        private bool OverReadLimit(long reads) => _request.ReadLimit is { } limit && reads > limit;

        private BuildResult Promotion(long reads) => Result(BuildStatus.NeedsLongAdmission, detail: $"The replay reads {reads} commits, more than the {_request.ReadLimit} short admission allows");

        // A cooperative stop: nothing new is read or published after the deadline
        private void CheckDeadline()
        {
            if (PastDeadline())
            {
                throw DeadlinePassed();
            }
        }

        private CheckpointDeclinedException DeadlinePassed() => new CheckpointDeclinedException(MaintenanceReason.JobDeadline, DeclineDependency.None, $"The job deadline {_request.Deadline:O} passed");

        private bool PastDeadline() => _request.Deadline is { } deadline && _context.Clock.GetUtcNow() >= deadline;

        private BuildResult DeadlineResult() => Result(BuildStatus.Declined, MaintenanceReason.JobDeadline, detail: $"The job deadline {_request.Deadline:O} passed");

        // A fetched root with its admission scan and marker
        private sealed class LocalRoot
        {
            public required LocalCheckpoint File { get; set; }

            public ScanResult? Scan { get; set; }

            public CoverageMarker? Marker { get; set; }

            public string Path => File.Path;

            public ObjectIdentity Identity => File.Identity;

            public long Version => File.Version;

            public void Dispose(CheckpointBuildContext context)
            {
                if (Scan != null)
                {
                    context.Memory.Release(Scan.ModelBytes);
                    Scan = null;
                }
                File.Dispose(context.Scratch);
            }
        }

        private async Task<(LocalRoot? Root, bool Blocked, BuildResult? Failure)> ChooseRootAsync(DeltaLogSegment segment, long target, CancellationToken cancellationToken)
        {
            var walk = new RootWalk(_request.KnownUnsupported);
            HeldRoot? held;
            try
            {
                held = await CheckpointProbe.WalkAsync(_context.Transport, _context.Table, _context.Scratch, _context.Memory, _context.Transfer,
                    segment.RootCandidates(target, target - 1), _cache, walk, CheckDeadline, cancellationToken);
            }
            finally
            {
                // A decline thrown by the walk reports what it saw so far
                _stale.AddRange(walk.Stale);
                _skipped.AddRange(walk.Skipped);
                _transferBytes += walk.TransferBytes;
                _newestValidRoot ??= walk.NewestValidRoot;
            }
            if (walk.Unknown != null)
            {
                // Never fall back past a candidate whose state is unknown
                return (null, walk.Blocked, Result(BuildStatus.Failed, MaintenanceReason.UnknownCheckpoint, detail: walk.Unknown));
            }
            return (held == null ? null : new LocalRoot() { File = held.File, Scan = held.Scan, Marker = held.Marker }, walk.Blocked, null);
        }

        private ReadOnlySpan<byte> KeyOf(ReadOnlySpan<byte> record)
        {
            var length = RecordKey.Length(record);
            if (_key.Length < length)
            {
                var grown = _context.Memory.Allocate(Math.Max(length, 256));
                FreeKey();
                _key = grown;
            }
            RecordKey.Write(record, _key);
            return _key.AsSpan(0, length);
        }

        private void FreeKey()
        {
            if (_key.Length > 0)
            {
                _context.Memory.Free(_key);
                _key = Array.Empty<byte>();
            }
        }

        private sealed class TailState : IDisposable
        {
            private readonly JobMemory _memory;

            public TailState(JobMemory memory)
            {
                _memory = memory;
            }

            public required TailReducer Map { get; init; }

            public CheckpointSorter? Sorter { get; set; }

            public byte[]? Protocol { get; private set; }

            public byte[]? MetaData { get; private set; }

            // The tail commits that held them
            public long? ProtocolVersion { get; private set; }

            public long? MetaDataVersion { get; private set; }

            // The root's, read only when the tail has none
            public byte[]? RootProtocol { get; private set; }

            public byte[]? RootMetaData { get; private set; }

            public long? FirstTimestamp { get; set; }

            public void KeepRoot(ReadOnlySpan<byte> record)
            {
                var kind = CheckpointRecord.Kind(record);
                if (kind == RecordKind.Protocol)
                {
                    RootProtocol = Copy(RootProtocol, record);
                }
                else if (kind == RecordKind.MetaData)
                {
                    RootMetaData = Copy(RootMetaData, record);
                }
            }

            // The last protocol and metaData of the tail, kept for the capability check
            public void Keep(ReadOnlySpan<byte> record, long version)
            {
                var kind = CheckpointRecord.Kind(record);
                if (kind == RecordKind.Protocol)
                {
                    Protocol = Copy(Protocol, record);
                    ProtocolVersion = version;
                }
                else if (kind == RecordKind.MetaData)
                {
                    MetaData = Copy(MetaData, record);
                    MetaDataVersion = version;
                }
            }

            private byte[] Copy(byte[]? old, ReadOnlySpan<byte> record)
            {
                var copy = _memory.Allocate(record.Length);
                record.CopyTo(copy);
                if (old != null)
                {
                    _memory.Free(old);
                }
                return copy;
            }

            public void Dispose()
            {
                Map.Dispose();
                Sorter?.Dispose();
                Sorter = null;
                if (Protocol != null)
                {
                    _memory.Free(Protocol);
                    Protocol = null;
                }
                if (MetaData != null)
                {
                    _memory.Free(MetaData);
                    MetaData = null;
                }
                if (RootProtocol != null)
                {
                    _memory.Free(RootProtocol);
                    RootProtocol = null;
                }
                if (RootMetaData != null)
                {
                    _memory.Free(RootMetaData);
                    RootMetaData = null;
                }
            }
        }

        private async Task<BuildResult> BuildFromAsync(DeltaLogSegment segment, long target, LocalRoot? root, CancellationToken cancellationToken)
        {
            var memory = _context.Memory;
            var rootVersion = root?.Version ?? -1;
            using var tail = new TailState(memory) { Map = new TailReducer(memory, _context.ArenaCap) };

            // The tail, latest record per key, into the map; an overflow drains it as a run and starts sort mode
            using (var encoder = new TailRecordEncoder(memory))
            {
                await using var window = new CommitReadAhead(segment.Storage,
                    index => segment.Listing.Commits.TryGetValue(rootVersion + 1 + index, out var file) ? file.IOEntry : null,
                    target - rootVersion, _context.ReadAhead, _context.Transfer, () => !PastDeadline(), cancellationToken);
                for (long version = rootVersion + 1; version <= target; version++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    CheckDeadline();
                    _tailCommit = ObjectIdentity.Of(_context.Table, segment.Listing.Commits[version]);
                    var listedSize = segment.Listing.Commits[version].IOEntry.Size ?? 0;
                    _transferBytes += listedSize;
                    using var opened = await window.NextAsync();
                    if (opened.Stopped)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        throw DeadlinePassed();
                    }
                    using var lines = DeltaLogSegment.Lines(opened.Stream ?? throw new DeltaVersionNotAvailableException($"Commit {version} was removed while it was read"),
                        memory, _context.LineCap, skipOversizedCommitInfo: true);
                    long lineOrdinal = 0;
                    long? commitTimestamp = null;
                    while (await lines.ReadLineAsync(cancellationToken))
                    {
                        if (BoundedLineReader.IsBlank(lines.Line.Span))
                        {
                            continue;
                        }
                        var ordinal = lineOrdinal++ * 8;
                        var version1 = version;
                        encoder.Encode(lines.Line.Span, record =>
                        {
                            tail.Keep(record, version1);
                            Add(tail, version1, ordinal++, record);
                        });
                        commitTimestamp ??= encoder.CommitTimestamp;
                    }
                    _skippedLines += lines.SkippedLines;
                    tail.FirstTimestamp ??= commitTimestamp;
                }
            }

            // Protocol and metaData at the target, from the tail or a pre-read of the root
            var protocolRecord = tail.Protocol;
            var metaDataRecord = tail.MetaData;
            if ((protocolRecord == null || metaDataRecord == null) && root != null)
            {
                PreRead(root, tail);
                protocolRecord ??= tail.RootProtocol;
                metaDataRecord ??= tail.RootMetaData;
            }
            if (protocolRecord == null || metaDataRecord == null)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.ValidationFailed, DeclineDependency.None, "No protocol or metaData at the target");
            }
            var protocol = CheckpointRecord.ToAction(protocolRecord).Protocol!;
            var metaData = CheckpointRecord.ToAction(metaDataRecord).MetaData!;
            // A protocol-dependent decline holds while these commits keep their identity
            foreach (var holder in new[] { tail.ProtocolVersion, tail.MetaDataVersion }.OfType<long>().Distinct())
            {
                _protocolCommits.Add(ObjectIdentity.Of(_context.Table, segment.Listing.Commits[holder]));
            }

            // Expiry on the options clock, then capability
            var buildNow = _context.Clock.GetUtcNow();
            var retention = CheckpointExpiry.Retention(metaData.Configuration);
            var cutoffTicks = CheckpointExpiry.CutoffTicks(buildNow, retention);
            var writerFeatures = CheckpointCapability.Check(protocol, metaData);

            // A root we did not write may repeat a key, which only sort mode resolves last-row-wins before expiry
            if (root != null && root.Marker == null)
            {
                StartSort(tail);
            }

            // The marker, known before the first row
            var mode = tail.Sorter != null ? BuildMode.Sort : BuildMode.Probe;
            var marker = CoverageMarker.Compute(target, buildNow.ToUnixTimeMilliseconds(), CheckpointExpiry.CutoffMs(cutoffTicks), root != null, rootVersion, root?.Marker,
                writerFeatures, tail.FirstTimestamp, mode == BuildMode.Sort);

            // Emit to scratch
            var codec = DeltaParquetCompression.Resolve(metaData.Configuration, _context.Codec, out _);
            var output = _context.Scratch.NewPath(".checkpoint.parquet");
            CheckpointWriteResult written;
            var writer = new CheckpointFileWriter(output, _context.Scratch.Ledger, memory, codec.Compression, marker.ToJson());
            try
            {
                void Emit(ReadOnlySpan<byte> record)
                {
                    CheckpointCapability.CheckEmitted(record, writerFeatures);
                    writer.Append(record);
                }

                if (mode == BuildMode.Probe)
                {
                    Emit(protocolRecord);
                    Emit(metaDataRecord);
                    if (root != null)
                    {
                        ReadRoot(root, record =>
                        {
                            var kind = CheckpointRecord.Kind(record);
                            if (kind is RecordKind.Protocol or RecordKind.MetaData || tail.Map.Contains(KeyOf(record)) || Dropped(kind, record, cutoffTicks))
                            {
                                return;
                            }
                            Emit(record);
                        }, cancellationToken);
                    }
                    tail.Map.ForEach((in ArenaEntry entry) =>
                    {
                        var kind = CheckpointRecord.Kind(entry.Record);
                        if (kind is RecordKind.Protocol or RecordKind.MetaData || Dropped(kind, entry.Record, cutoffTicks))
                        {
                            return;
                        }
                        Emit(entry.Record);
                    });
                }
                else
                {
                    var sorter = tail.Sorter!;
                    if (root != null)
                    {
                        long rowOrdinal = 0;
                        ReadRoot(root, record => Add(tail, rootVersion, rowOrdinal++, record), cancellationToken);
                    }
                    sorter.Reduce(tail.Map, (in ArenaEntry entry) =>
                    {
                        if (!Dropped(CheckpointRecord.Kind(entry.Record), entry.Record, cutoffTicks))
                        {
                            Emit(entry.Record);
                        }
                    }, cancellationToken);
                }
                _context.BuildFaultForTests?.Invoke("writer close");
                written = writer.Close();
            }
            finally
            {
                writer.Dispose();
                _context.Scratch.Adopt(output, writer.Charged);
            }
            var spill = tail.Sorter?.SpillBytes ?? 0;
            var rootVersionUsed = root?.Version;
            tail.Dispose();
            root?.Dispose(_context);

            // Validate before anything is published
            _context.BeforeValidationForTests?.Invoke(output);
            var validationStarted = System.Diagnostics.Stopwatch.GetTimestamp();
            var failure = Validate(output, written, marker);
            var validationMs = System.Diagnostics.Stopwatch.GetElapsedTime(validationStarted).TotalMilliseconds;
            if (failure != null)
            {
                return Result(BuildStatus.Failed, MaintenanceReason.ValidationFailed, detail: failure) with { Mode = mode, RootVersion = rootVersionUsed, SpillBytes = spill };
            }
            var descriptor = new CheckpointDescriptor(target, output, written.OutputBytes, written.Actions, written.AddFiles, marker, written.Sha256);
            if (_context.Publish)
            {
                CheckDeadline();
                var published = await CheckpointPublisher.PublishAsync(_context.Transport, _context.Scratch, memory, descriptor, _context.FreeSpaceForTests, cancellationToken, _context.PublishFaultForTests, _cache);
                if (_context.Transport.NativePath(_context.Transport.Table) == null)
                {
                    _transferBytes += descriptor.SizeInBytes;
                }
                if (!published.Published)
                {
                    return Result(BuildStatus.Failed, MaintenanceReason.PublishFailed, detail: published.Failure) with { Mode = mode, RootVersion = rootVersionUsed, SpillBytes = spill };
                }
            }
            return new BuildResult(BuildStatus.Completed, descriptor)
            {
                Mode = mode,
                RootVersion = rootVersionUsed,
                LegacyGapKept = _legacyGapKept,
                SpillBytes = spill,
                TransferBytes = _transferBytes,
                SkippedLines = _skippedLines,
                Skipped = _skipped,
                Candidates = _candidates,
                TargetCommit = _targetCommit,
                Stale = _stale,
                ValidationMs = validationMs,
                Fence = _fence
            };
        }

        // A full map drains as a sorted run, then takes the record into its emptied arena
        private void Add(TailState tail, long version, long ordinal, ReadOnlySpan<byte> record)
        {
            var key = KeyOf(record);
            if (!tail.Map.TryAdd(key, version, ordinal, record))
            {
                StartSort(tail).Spill(tail.Map);
                // An empty reducer takes any record
                if (!tail.Map.TryAdd(key, version, ordinal, record))
                {
                    throw new InvalidOperationException("The emptied tail reducer refused a record");
                }
            }
        }

        // Sort mode starts at the first spill, or before a root we did not write is read
        private CheckpointSorter StartSort(TailState tail)
        {
            return tail.Sorter ??= new CheckpointSorter(_context.Scratch, _context.Memory) { FaultForTests = _context.BuildFaultForTests };
        }

        // Expired tombstones and removed domains are never written
        private static bool Dropped(RecordKind kind, ReadOnlySpan<byte> record, Int128 cutoffTicks)
        {
            return kind switch
            {
                RecordKind.Remove => CheckpointExpiry.Expired(CheckpointRecord.DeletionTimestamp(record), cutoffTicks),
                RecordKind.Domain => !DeltaReconcileRules.IsWritten(new DeltaDomainMetadataAction() { Removed = CheckpointRecord.DomainRemoved(record) }),
                _ => false
            };
        }

        private void ReadRoot(LocalRoot root, RecordSink sink, CancellationToken cancellationToken)
        {
            using var reader = new BoundedCheckpointReader(root.Path, root.Scan!, _context.Memory);
            reader.Read(sink, cancellationToken);
        }

        // The root's protocol and metaData, projecting only their leaves, into charged copies the tail state owns
        private void PreRead(LocalRoot root, TailState tail)
        {
            var scan = root.Scan!;
            var leaves = scan.Projection.Leaves.Where(x => x.Role == ProjectedRole.Field
                && (x.Field is >= Leaf.ProtocolMinReader and <= Leaf.ProtocolWriterFeature || x.Field is >= Leaf.MetaId and <= Leaf.MetaConfigurationValue)).ToList();
            using var reader = new BoundedCheckpointReader(root.Path, scan with { Projection = scan.Projection.WithLeaves(leaves) }, _context.Memory);
            reader.Read(record => tail.KeepRoot(record));
        }

        /// <summary>
        /// The output is scanned, read back in full, and must match what was written; null when it holds.
        /// </summary>
        private string? Validate(string path, CheckpointWriteResult written, CoverageMarker marker)
        {
            var memory = _context.Memory;
            var previousKey = Array.Empty<byte>();
            var validationKey = Array.Empty<byte>();
            ScanResult scan;
            try
            {
                scan = CheckpointPageScan.Scan(path, memory);
            }
            catch (CheckpointDeclinedException e)
            {
                return $"The output is not admitted by the scan: {e.Message}";
            }
            try
            {
                var projection = scan.Projection;
                if (projection.LeafCount != CheckpointSchema.LeafCount || projection.Leaves.Count != CheckpointSchema.LeafCount || projection.Leaves.Any(x => x.Role != ProjectedRole.Field))
                {
                    return "The output schema is not the checkpoint schema";
                }
                if (scan.Footer.RowGroupRows.Sum() != written.Actions)
                {
                    return "The output's row groups hold a different row count";
                }
                if (scan.Footer.Chunks.Any(group => group.Any(chunk => chunk?.HasPageIndex == true)))
                {
                    return "The output has a page index";
                }
                if (scan.Footer.Marker != marker.ToJson() || CoverageMarker.Parse(scan.Footer.Marker) == null)
                {
                    return "The output's marker is missing or differs";
                }

                long actions = 0, adds = 0, protocols = 0, metaDatas = 0;
                var ordered = true;
                var previousLength = -1;
                using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                using (var reader = new BoundedCheckpointReader(path, scan, memory))
                {
                    reader.Read(record =>
                    {
                        CheckpointFileWriter.AppendDigest(hash, record);
                        actions++;
                        switch (CheckpointRecord.Kind(record))
                        {
                            case RecordKind.Add:
                                adds++;
                                break;
                            case RecordKind.Protocol:
                                protocols++;
                                break;
                            case RecordKind.MetaData:
                                metaDatas++;
                                break;
                        }
                        if (marker.Sorted)
                        {
                            // Two charged key buffers, swapped per record
                            var keyLength = RecordKey.Length(record);
                            if (validationKey.Length < keyLength)
                            {
                                var grown = memory.Allocate(Math.Max(keyLength, 256));
                                if (validationKey.Length > 0)
                                {
                                    memory.Free(validationKey);
                                }
                                validationKey = grown;
                            }
                            RecordKey.Write(record, validationKey);
                            ordered &= previousLength < 0 || previousKey.AsSpan(0, previousLength).SequenceCompareTo(validationKey.AsSpan(0, keyLength)) < 0;
                            (previousKey, validationKey) = (validationKey, previousKey);
                            previousLength = keyLength;
                        }
                    });
                }
                if (actions != written.Actions || adds != written.AddFiles || !hash.GetHashAndReset().AsSpan().SequenceEqual(written.Sha256))
                {
                    return "The output read back differs from what was written";
                }
                if (protocols != 1 || metaDatas != 1)
                {
                    return $"The output has {protocols} protocol and {metaDatas} metaData actions";
                }
                if (!ordered)
                {
                    return "The keys of a sorted output do not strictly increase";
                }
                return null;
            }
            catch (CheckpointDeclinedException e)
            {
                return $"The output fails to read back: {e.Message}";
            }
            finally
            {
                memory.Release(scan.ModelBytes);
                if (previousKey.Length > 0)
                {
                    memory.Free(previousKey);
                }
                if (validationKey.Length > 0)
                {
                    memory.Free(validationKey);
                }
            }
        }
    }
}
