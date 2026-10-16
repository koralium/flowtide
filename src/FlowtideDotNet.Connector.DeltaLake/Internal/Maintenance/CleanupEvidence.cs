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
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// What one destructive gate concluded from current storage evidence, a finite set.
    /// </summary>
    internal enum GateVerdict : byte
    {
        Passed = 0,
        // Nothing concluded: the run keeps its state and tries again
        ListingFailed = 1,
        Unavailable = 2,
        Preempted = 3,
        // An object changed or vanished between the listing and its read
        Changed = 4,
        // The anchor is above the listed head
        NotPublished = 5,
        NeedsLongAdmission = 6,
        // Stops deletion: durable runs suspend with their authority, volatile runs stop and drop
        AnchorMissing = 7,
        AnchorReplaced = 8,
        // An object the cached protocol or metadata facts came from is gone or changed
        SourceReplaced = 9,
        TableChanged = 10,
        // The head fell below the last validated head, or the frontier commit changed
        Rollback = 11,
        NotOwn = 12,
        StageMismatch = 13,
        Incapable = 14,
        // Stops volatile work; an advance handles these itself
        ForeignHead = 15,
        NewEpoch = 16,
        // The storage is not the instance classified at construction
        StorageChanged = 17,
        // The listing's checks did not complete, or its successor probe started before the last gate
        StaleListing = 18,
        // This admission's counted reads cannot cover the gate; a fresh admission can
        ReadsSpent = 19
    }

    /// <summary>
    /// What a run does with a verdict.
    /// </summary>
    internal enum GateAction : byte
    {
        Proceed,
        // Keep everything and try again later
        Retry,
        // Run again through long admission
        Promote,
        // A volatile run stops and drops its work
        Stop,
        // A durable run stops issuing and keeps its authority
        Suspend,
        // A durable advance applies its own foreign-change handling
        Rebase
    }

    internal sealed record GateResult(GateVerdict Verdict, string? Detail = null)
    {
        public ListingCheck Listing { get; init; }

        public CapabilityCheck Capability { get; init; }

        public IssuanceStop Stop { get; init; }

        // The listed head the verdict was reached at, -1 when no listing was used
        public long Head { get; init; } = -1;

        // The head commit's complete proof when the gate read it
        public ContentProof? HeadProof { get; init; }

        // Set when establishment passed
        public CleanupEvidence? Evidence { get; init; }

        public bool Passed => Verdict == GateVerdict.Passed;

        // The run's kind, from its evidence once it has one
        public bool Durable { get; init; }

        public GateAction Action => Verdict switch
        {
            GateVerdict.Passed => GateAction.Proceed,
            GateVerdict.ListingFailed or GateVerdict.Unavailable or GateVerdict.Preempted or GateVerdict.Changed or GateVerdict.NotPublished
                or GateVerdict.StaleListing or GateVerdict.ReadsSpent => GateAction.Retry,
            GateVerdict.NeedsLongAdmission => GateAction.Promote,
            GateVerdict.ForeignHead or GateVerdict.NewEpoch => Durable ? GateAction.Rebase : GateAction.Stop,
            // A replaced instance invalidates the evidence; durable authority waits for a replacement sink
            GateVerdict.StorageChanged => Durable ? GateAction.Retry : GateAction.Stop,
            _ => Durable ? GateAction.Suspend : GateAction.Stop
        };

        public MaintenanceReason Reason => Verdict switch
        {
            GateVerdict.Passed or GateVerdict.NeedsLongAdmission => MaintenanceReason.None,
            GateVerdict.ListingFailed or GateVerdict.StorageChanged or GateVerdict.StaleListing => MaintenanceReason.ListingIneligible,
            GateVerdict.Unavailable or GateVerdict.Changed or GateVerdict.NotPublished or GateVerdict.ReadsSpent => MaintenanceReason.Transient,
            GateVerdict.Preempted => Stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None,
            GateVerdict.Incapable => CleanupCapability.ReasonOf(Capability, Stop),
            GateVerdict.NotOwn or GateVerdict.StageMismatch => MaintenanceReason.CleanupGate,
            _ => MaintenanceReason.HistoryChanged
        };

        /// <summary>
        /// The verdict for a listing that failed its checks, for a run of the given kind.
        /// </summary>
        public static GateResult OfListing(ListingCheck check, bool durable) => new GateResult(check == ListingCheck.StorageChanged ? GateVerdict.StorageChanged : GateVerdict.ListingFailed,
            $"The listing failed its {check} check") { Listing = check, Durable = durable };
    }

    /// <summary>
    /// The current storage evidence one cleanup run relies on: its anchor, the table's capability facts, the frontier X and the last validated head; every destructive gate revalidates it.
    /// </summary>
    internal sealed class CleanupEvidence
    {
        private ContentProof? _frontier;

        // The listing sequence when the last gate ended
        private long _gateSequence;

        private CleanupEvidence(CommitAnchor anchor, TableCapability capability, long head, bool durable)
        {
            Anchor = anchor;
            Capability = capability;
            Frontier = anchor.Version;
            LastValidatedHead = head;
            Durable = durable;
            _gateSequence = LogEvidence.LastSequence;
        }

        public CommitAnchor Anchor { get; }

        // Facts trusted only while every source's complete content still hashes the same
        public TableCapability Capability { get; }

        public string TableId => Anchor.TableId;

        // A durable run suspends where a volatile run stops and drops
        public bool Durable { get; }

        /// <summary>
        /// The frontier: the newest version the run's protection covers.
        /// </summary>
        public long Frontier { get; private set; }

        public long LastValidatedHead { get; private set; }

        /// <summary>
        /// The verdict that stopped or suspended this evidence; every later gate repeats it.
        /// </summary>
        public GateVerdict? StoppedBy { get; private set; }

        /// <summary>
        /// Establishes evidence at a listing ReadLogAsync checked: the capability at its head, a strict own proof of the anchor and an own head in the anchor's epoch; never throws for runtime failures.
        /// </summary>
        public static async Task<GateResult> EstablishAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, long anchorVersion, string? expectedStageId, bool durable,
            CleanupContext context, TableCapability? capability = null)
        {
            var result = await EstablishCoreAsync(configuration, storage, log, anchorVersion, expectedStageId, durable, context, capability, null);
            return result with { Durable = durable };
        }

        /// <summary>
        /// Establishes evidence from a capability checked at this listing and the commits its replay proved; never throws for runtime failures.
        /// </summary>
        public static async Task<GateResult> EstablishAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, long anchorVersion, string? expectedStageId, bool durable,
            CleanupContext context, TableCapability capability, IReadOnlyDictionary<long, ContentProof> proved)
        {
            // Proofs made at another listing prove nothing at this one
            var replayed = capability.ListingSequence == log.Sequence ? proved : null;
            var result = await EstablishCoreAsync(configuration, storage, log, anchorVersion, expectedStageId, durable, context, capability, replayed);
            return result with { Durable = durable };
        }

        private static async Task<GateResult> EstablishCoreAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, long anchorVersion, string? expectedStageId, bool durable,
            CleanupContext context, TableCapability? capability, IReadOnlyDictionary<long, ContentProof>? replayed)
        {
            var head = log.Head;
            try
            {
                if (Unusable(log, 0) is { } unusable)
                {
                    return unusable;
                }
                if (configuration.StorageChanged(storage))
                {
                    return new GateResult(GateVerdict.StorageChanged) { Listing = ListingCheck.StorageChanged, Head = head };
                }
                if (anchorVersion > head)
                {
                    return new GateResult(GateVerdict.NotPublished, $"Commit {anchorVersion} is above the head {head}") { Head = head };
                }
                if (anchorVersion < 0 || ContentProof.Find(log.Listing, anchorVersion, false) == null)
                {
                    return new GateResult(GateVerdict.AnchorMissing, $"Commit {anchorVersion} is not listed") { Head = head };
                }
                if (log.CheckRange(anchorVersion, head) is var range && range != ListingCheck.Passed)
                {
                    return new GateResult(GateVerdict.ListingFailed, $"Commits from {anchorVersion} to {head} are not contiguous") { Listing = range, Head = head };
                }

                IReadOnlyDictionary<long, ContentProof> proofs = new Dictionary<long, ContentProof>();
                if (capability == null)
                {
                    var checkedCapability = await CleanupCapability.CheckAsync(configuration, storage, log, context, prove: new[] { anchorVersion });
                    if (checkedCapability.Check != CapabilityCheck.Capable)
                    {
                        return OfCapability(checkedCapability, head);
                    }
                    capability = checkedCapability.Capability!;
                    proofs = checkedCapability.Proofs;
                }
                else if (await CachedAsync(storage, log, capability, context) is { } stale)
                {
                    return stale;
                }
                else if (replayed != null)
                {
                    proofs = replayed;
                }

                var table = configuration.Store.Table;
                var anchorProof = await ProofOf(table, storage, log, anchorVersion, proofs, context);
                if (anchorProof.Check != AnchorCheck.Proven)
                {
                    return OfProof(anchorProof, anchorVersion, head);
                }
                var own = CommitAnchor.TryCreate(capability.TableId, anchorProof.Proof!, expectedStageId, out var anchor);
                if (own != AnchorCheck.Proven)
                {
                    return new GateResult(own == AnchorCheck.StageMismatch ? GateVerdict.StageMismatch : GateVerdict.NotOwn,
                        $"Commit {anchorVersion} {(own == AnchorCheck.StageMismatch ? "belongs to another attempt" : "does not prove an own stage id and epoch")}") { Head = head };
                }

                var headProof = head == anchorVersion ? anchorProof : await ProofOf(table, storage, log, head, proofs, context);
                if (headProof.Check != AnchorCheck.Proven)
                {
                    return OfProof(headProof, head, head);
                }
                // The head read now must be the head the cached facts were established at
                if (capability.HeadProof is { } cachedHead && !cachedHead.SameContent(headProof.Proof!.Length, headProof.Proof.Sha256))
                {
                    return new GateResult(GateVerdict.Changed, $"Head {head} has other content than when the capability was established") { Head = head };
                }
                var epoch = Epoch(anchor!, headProof.Proof!);
                if (epoch != GateVerdict.Passed)
                {
                    return new GateResult(epoch, $"Head {head} is not an own commit of the anchor's epoch {anchor!.AdoptedAt}") { Head = head, HeadProof = headProof.Proof };
                }
                return new GateResult(GateVerdict.Passed) { Head = head, HeadProof = headProof.Proof, Evidence = new CleanupEvidence(anchor!, capability, head, durable) };
            }
            catch (CleanupPreemptedException e)
            {
                return new GateResult(GateVerdict.Preempted, e.Message) { Stop = e.Stop, Head = head };
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                return new GateResult(GateVerdict.Preempted, "The worker stopped") { Stop = IssuanceStop.Stopped, Head = head };
            }
            catch (Exception e)
            {
                return new GateResult(GateVerdict.Unavailable, $"{e.GetType().Name}: {e.Message}") { Head = head };
            }
        }

        // A proof this establishment's own replay made, or a complete read of its own
        private static async Task<CommitProof> ProofOf(IOPath table, IFileStorage storage, LogEvidence log, long version, IReadOnlyDictionary<long, ContentProof> proofs, CleanupContext context)
        {
            if (proofs.TryGetValue(version, out var replayed))
            {
                return new CommitProof(AnchorCheck.Proven, replayed);
            }
            return await CommitAnchor.ProveAsync(table, storage, log, version, context);
        }

        // A cached capability's head and sources as this listing shows them, rehashed when another listing established them
        private static async Task<GateResult?> CachedAsync(IFileStorage storage, LogEvidence log, TableCapability capability, CleanupContext context)
        {
            var head = log.Head;
            if (capability.Version != head)
            {
                return new GateResult(GateVerdict.Changed, $"The capability holds at {capability.Version}, the listing's head is {head}") { Head = head };
            }
            var listed = capability.HeadProof == null ? capability.Sources : capability.Sources.Append(capability.HeadProof);
            foreach (var proof in listed)
            {
                if (!proof.ListedUnchanged(ContentProof.Find(log.Listing, proof.Version, proof.IsCheckpoint)))
                {
                    return new GateResult(GateVerdict.Changed, $"{Describe(proof)} is not listed as the capability saw it") { Head = head };
                }
            }
            if (capability.ListingSequence == log.Sequence)
            {
                return null;
            }
            foreach (var source in capability.Sources)
            {
                var read = await ProofReader.ReadAsync(storage, source.Path, source.Version, source.Listed.Size >= 0 ? source.Listed.Size : null, false, context);
                if (read.Status != ProofStatus.Read)
                {
                    return OfRead(read, source, head);
                }
                if (!source.SameContent(read.Length, read.Sha256))
                {
                    return new GateResult(GateVerdict.Changed, $"{Describe(source)} has other content than when the capability was established") { Head = head };
                }
            }
            return null;
        }

        // A capability check that did not establish the facts; only table facts are incapable
        private static GateResult OfCapability(CapabilityResult result, long head) => result.Check switch
        {
            CapabilityCheck.NeedsLongAdmission or CapabilityCheck.OverReadLimit => new GateResult(GateVerdict.NeedsLongAdmission, result.Detail) { Capability = result.Check, Head = head },
            CapabilityCheck.ReadsSpent => new GateResult(GateVerdict.ReadsSpent, result.Detail) { Capability = result.Check, Head = head },
            CapabilityCheck.Preempted => new GateResult(GateVerdict.Preempted, result.Detail) { Capability = result.Check, Stop = result.Stop, Head = head },
            CapabilityCheck.Changed => new GateResult(GateVerdict.Changed, result.Detail) { Capability = result.Check, Head = head },
            CapabilityCheck.Unavailable => new GateResult(GateVerdict.Unavailable, result.Detail) { Capability = result.Check, Head = head },
            _ => new GateResult(GateVerdict.Incapable, result.Detail) { Capability = result.Check, Head = head }
        };

        private static GateResult OfProof(CommitProof proof, long version, long head) => proof.Check switch
        {
            AnchorCheck.Preempted => new GateResult(GateVerdict.Preempted, $"Reading commit {version} stopped") { Stop = proof.Stop, Head = head },
            AnchorCheck.ReadsSpent => new GateResult(GateVerdict.ReadsSpent, $"Reading commit {version} needs a read this admission has spent") { Head = head },
            AnchorCheck.NotPublished => new GateResult(GateVerdict.NotPublished, $"Commit {version} is above the head {head}") { Head = head },
            AnchorCheck.Missing => new GateResult(GateVerdict.AnchorMissing, $"Commit {version} is missing") { Head = head },
            _ => new GateResult(GateVerdict.Changed, $"Commit {version} is not the listed size") { Head = head }
        };

        // A rehash that read nothing: a stop, a spent admission, or an object gone after the listing
        private static GateResult OfRead(ProofRead read, ContentProof proof, long head) => read.Status switch
        {
            ProofStatus.Preempted => new GateResult(GateVerdict.Preempted, $"Rehashing {Describe(proof)} stopped") { Stop = read.Stop, Head = head },
            ProofStatus.ReadsSpent => new GateResult(GateVerdict.ReadsSpent, $"Rehashing {Describe(proof)} needs a read this admission has spent") { Head = head },
            _ => new GateResult(GateVerdict.Changed, $"{Describe(proof)} is gone") { Head = head }
        };

        // Stage-only evidence, or a listing not probed after the given sequence
        private static GateResult? Unusable(LogEvidence log, long after)
        {
            if (log.Purpose != EvidencePurpose.Absence)
            {
                return new GateResult(GateVerdict.ListingFailed, "The listing serves only stage-file removal") { Listing = ListingCheck.StoreIneligible, Head = log.Head };
            }
            if (log.Sequence <= after)
            {
                return new GateResult(GateVerdict.StaleListing, log.Sequence == 0 ? "The listing never passed its head, fence and successor checks" : "The listing's successor probe started before the last gate ended")
                {
                    Head = log.Head
                };
            }
            return null;
        }

        // The head continues the anchor's own epoch: an own commit with the same adoption version
        private static GateVerdict Epoch(CommitAnchor anchor, ContentProof head)
        {
            if (head.Provenance is not { IsOwn: true } provenance)
            {
                return GateVerdict.ForeignHead;
            }
            return provenance.AdoptedAt == anchor.AdoptedAt ? GateVerdict.Passed : GateVerdict.NewEpoch;
        }

        /// <summary>
        /// Whether a claim such as a discard hint's names this evidence's table and epoch; an unknown claim never matches.
        /// </summary>
        public GateVerdict CheckClaim(string? tableId, long? adoptedAt)
        {
            if (tableId == null || !string.Equals(tableId, TableId, StringComparison.Ordinal))
            {
                return GateVerdict.TableChanged;
            }
            // An unknown epoch never matches
            return adoptedAt == null || adoptedAt != Anchor.AdoptedAt ? GateVerdict.NewEpoch : GateVerdict.Passed;
        }

        /// <summary>
        /// Moves X up to a commit a passed gate proved; false when the proof is not an own commit in (X, last validated head] of the anchor's epoch.
        /// </summary>
        public bool ExtendFrontier(ContentProof proof)
        {
            if (StoppedBy != null || proof.IsCheckpoint || proof.Version <= Frontier || proof.Version > LastValidatedHead || Epoch(Anchor, proof) != GateVerdict.Passed)
            {
                return false;
            }
            _frontier = proof;
            Frontier = proof.Version;
            return true;
        }

        /// <summary>
        /// Revalidates the evidence against a listing ReadLogAsync checked after the last gate: no rollback, the anchor, frontier and capability sources listed and rehashed in full, and an own head in the anchor's epoch; never throws for runtime failures.
        /// </summary>
        public async Task<GateResult> ValidateAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupContext context, long? relyFrom = null, GateReads? reads = null)
        {
            if (StoppedBy is { } stopped)
            {
                return new GateResult(stopped, "An earlier gate stopped this run") { Head = log.Head, Durable = Durable };
            }
            var result = (Unusable(log, _gateSequence) ?? await ValidateCoreAsync(configuration, storage, log, context, relyFrom, reads)) with { Durable = Durable };
            // The next gate needs a listing probed after this one
            _gateSequence = LogEvidence.LastSequence;
            if (result.Action is GateAction.Stop or GateAction.Suspend)
            {
                StoppedBy = result.Verdict;
            }
            else if (result.Passed)
            {
                LastValidatedHead = result.Head;
            }
            return result;
        }

        private async Task<GateResult> ValidateCoreAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupContext context, long? relyFrom, GateReads? reads)
        {
            var head = log.Head;
            try
            {
                if (configuration.StorageChanged(storage))
                {
                    return new GateResult(GateVerdict.StorageChanged) { Listing = ListingCheck.StorageChanged, Head = head };
                }
                if (head < LastValidatedHead)
                {
                    return new GateResult(GateVerdict.Rollback, $"The head {head} is below the validated head {LastValidatedHead}") { Head = head };
                }

                // Cheap listing checks before any byte is read
                var checks = new List<(ContentProof Proof, GateVerdict Missing, GateVerdict Replaced)>
                {
                    (Anchor.Proof, GateVerdict.AnchorMissing, GateVerdict.AnchorReplaced)
                };
                if (_frontier != null)
                {
                    checks.Add((_frontier, GateVerdict.Rollback, GateVerdict.Rollback));
                }
                foreach (var source in Capability.Sources)
                {
                    if (!checks.Any(x => x.Proof.Path.Full == source.Path.Full))
                    {
                        checks.Add((source, GateVerdict.SourceReplaced, GateVerdict.SourceReplaced));
                    }
                }
                foreach (var (proof, missing, replaced) in checks)
                {
                    var file = ContentProof.Find(log.Listing, proof.Version, proof.IsCheckpoint);
                    if (file == null)
                    {
                        return new GateResult(missing, $"{Describe(proof)} is not listed") { Head = head };
                    }
                    if (!proof.ListedUnchanged(file))
                    {
                        return new GateResult(replaced, $"{Describe(proof)} is listed with another size, time or MD5") { Head = head };
                    }
                }
                var from = Math.Min(relyFrom ?? Anchor.Version, Anchor.Version);
                if (log.CheckRange(from, head) is var range && range != ListingCheck.Passed)
                {
                    return new GateResult(GateVerdict.ListingFailed, $"Commits from {from} to {head} are not contiguous") { Listing = range, Head = head };
                }

                // Every relied-on object is rehashed in full
                ContentProof? headProof = null;
                foreach (var (proof, missing, replaced) in checks)
                {
                    var atHead = !proof.IsCheckpoint && proof.Version == head;
                    var read = reads != null ? await reads.ReadAsync(storage, proof.Path, proof.Version, proof.IsCheckpoint, proof.Listed.Size >= 0 ? proof.Listed.Size : null, context)
                        : await ProofReader.ReadAsync(storage, proof.Path, proof.Version, proof.Listed.Size >= 0 ? proof.Listed.Size : null, atHead, context);
                    if (read.Status == ProofStatus.Missing)
                    {
                        return new GateResult(missing, $"{Describe(proof)} is gone") { Head = head };
                    }
                    if (read.Status != ProofStatus.Read)
                    {
                        return OfRead(read, proof, head);
                    }
                    if (!proof.SameContent(read.Length, read.Sha256))
                    {
                        return new GateResult(replaced, $"{Describe(proof)} has other content") { Head = head };
                    }
                    if (atHead)
                    {
                        headProof = new ContentProof(proof.Path, proof.Version, false, proof.Listed, read.Length, read.Sha256!) { Provenance = read.Provenance };
                    }
                }
                if (headProof == null)
                {
                    var read = reads != null ? await reads.ProveAsync(configuration.Store.Table, storage, log, head, context) : await CommitAnchor.ProveAsync(configuration.Store.Table, storage, log, head, context);
                    if (read.Check != AnchorCheck.Proven)
                    {
                        // The head was listed, so its absence or another size is a change after the listing
                        return read.Check is AnchorCheck.Preempted or AnchorCheck.ReadsSpent ? OfProof(read, head, head) : new GateResult(GateVerdict.Changed, $"Head {head} changed after the listing") { Head = head };
                    }
                    headProof = read.Proof!;
                }

                var epoch = Epoch(Anchor, headProof);
                if (epoch != GateVerdict.Passed)
                {
                    return await ForeignAsync(configuration, storage, log, context, epoch, headProof);
                }
                return new GateResult(GateVerdict.Passed) { Head = head, HeadProof = headProof };
            }
            catch (CleanupPreemptedException e)
            {
                return new GateResult(GateVerdict.Preempted, e.Message) { Stop = e.Stop, Head = head };
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                return new GateResult(GateVerdict.Preempted, "The worker stopped") { Stop = IssuanceStop.Stopped, Head = head };
            }
            catch (Exception e)
            {
                return new GateResult(GateVerdict.Unavailable, $"{e.GetType().Name}: {e.Message}") { Head = head };
            }
        }

        // A volatile run stops on a foreign head or new epoch; a durable run first rereads the table id and capability at the head
        private async Task<GateResult> ForeignAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupContext context, GateVerdict epoch, ContentProof headProof)
        {
            var head = log.Head;
            var detail = $"Head {head} is not an own commit of the anchor's epoch {Anchor.AdoptedAt}";
            if (!Durable)
            {
                return new GateResult(epoch, detail) { Head = head, HeadProof = headProof };
            }
            var current = await CleanupCapability.CheckAsync(configuration, storage, log, context);
            return current.Check switch
            {
                CapabilityCheck.Capable when current.Capability!.TableId != TableId =>
                    new GateResult(GateVerdict.TableChanged, $"The table id at {head} is not the anchor's") { Head = head, HeadProof = headProof },
                CapabilityCheck.Capable => new GateResult(epoch, detail) { Head = head, HeadProof = headProof },
                _ => OfCapability(current, head)
            };
        }

        private static string Describe(ContentProof proof) => proof.IsCheckpoint ? $"Checkpoint {proof.Version}" : $"Commit {proof.Version}";
    }

    /// <summary>
    /// One gate's complete reads, so each relied-on object is read once per gate.
    /// </summary>
    internal sealed class GateReads
    {
        private readonly Dictionary<string, ProofRead> _reads = new Dictionary<string, ProofRead>(StringComparer.Ordinal);

        /// <summary>
        /// A complete read, a commit's with its first line proven; only completed reads are kept.
        /// </summary>
        public async Task<ProofRead> ReadAsync(IFileStorage storage, IOPath path, long version, bool isCheckpoint, long? listedSize, CleanupContext context)
        {
            if (_reads.TryGetValue(path.Full, out var kept))
            {
                return kept;
            }
            var read = await ProofReader.ReadAsync(storage, path, version, listedSize, proveFirstLine: !isCheckpoint, context);
            if (read.Status == ProofStatus.Read)
            {
                _reads[path.Full] = read;
            }
            return read;
        }

        /// <summary>
        /// A listed commit's proof as CommitAnchor.ProveAsync makes it, from this gate's reads.
        /// </summary>
        public async Task<CommitProof> ProveAsync(IOPath table, IFileStorage storage, LogEvidence log, long version, CleanupContext context)
        {
            if (version > log.Head)
            {
                return new CommitProof(AnchorCheck.NotPublished, null);
            }
            if (ContentProof.Find(log.Listing, version, false) is not { } file)
            {
                return new CommitProof(AnchorCheck.Missing, null);
            }
            var listed = file.IOEntry.Size;
            var read = await ReadAsync(storage, file.IOEntry.Path, version, false, listed, context);
            switch (read.Status)
            {
                case ProofStatus.Missing:
                    return new CommitProof(AnchorCheck.Missing, null);
                case ProofStatus.Preempted:
                    return new CommitProof(AnchorCheck.Preempted, null, read.Stop);
                case ProofStatus.ReadsSpent:
                    return new CommitProof(AnchorCheck.ReadsSpent, null);
            }
            if (listed != null && read.Length != listed)
            {
                return new CommitProof(AnchorCheck.Changed, null);
            }
            return new CommitProof(AnchorCheck.Proven, new ContentProof(file.IOEntry.Path, version, false, ObjectIdentity.Of(table, file), read.Length, read.Sha256!) { Provenance = read.Provenance });
        }
    }
}
