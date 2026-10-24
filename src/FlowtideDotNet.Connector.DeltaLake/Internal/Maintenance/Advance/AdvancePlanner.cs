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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// Plans the files an advance to a boundary may delete: candidates before C minus the references retained at C, window by window against one sealed protection set.
    /// </summary>
    internal static class AdvancePlanner
    {
        private enum ObjectRead : byte
        {
            Read,
            // Gone after the listing
            Missing,
            // Not its listed size
            Changed,
            // The admission's counted reads were spent
            Spent,
            // A path protection cannot use
            Declined
        }

        /// <summary>
        /// The plan under the default bounds; never throws except an OperationCanceledException from the context's stop.
        /// </summary>
        public static Task<AdvancePlan> PlanAsync(CleanupConfiguration configuration, IFileStorage meteredStorage, IOPath table,
            AdvanceBoundary boundary, LogClassification classes, CleanupContext context)
        {
            return PlanAsync(configuration, meteredStorage, table, boundary, classes, context, AdvanceWindowLimits.Default);
        }

        /// <summary>
        /// The plan under the given bounds; never throws except an OperationCanceledException from the context's stop.
        /// </summary>
        internal static async Task<AdvancePlan> PlanAsync(CleanupConfiguration configuration, IFileStorage meteredStorage, IOPath table,
            AdvanceBoundary boundary, LogClassification classes, CleanupContext context, AdvanceWindowLimits limits)
        {
            var o = boundary.O;
            var c = boundary.C;
            var listing = boundary.Log.Listing;
            if (o < 0 || o >= c || boundary.Checkpoint.Version != c || c > boundary.Head || boundary.A > c)
            {
                return Declined(MaintenanceReason.Unexpected, $"The boundary from {o} to checkpoint {boundary.Checkpoint.Version} at {c} below head {boundary.Head}, adopted at {boundary.A}, is not a range");
            }
            var tombstones = Tombstones(boundary);
            if (tombstones && boundary.OldestRoot?.Version != o)
            {
                return Declined(MaintenanceReason.MissingHistory, $"The unmarked oldest root at {o} is not listed");
            }
            var first = FirstCandidate(boundary);
            if (boundary.Log.CheckRange(first, c - 1) != ListingCheck.Passed)
            {
                return Declined(MaintenanceReason.MissingHistory, $"Commits from {first} to {c - 1} are not all listed");
            }
            if (!listing.Commits.TryGetValue(c, out var boundaryCommit))
            {
                return Declined(MaintenanceReason.MissingHistory, $"Commit {c} is not listed");
            }
            var newest = NewestAbove(boundary);
            // The tail, empty without such a checkpoint
            var tailFrom = (newest?.Version ?? boundary.Head) + 1;
            if (boundary.Log.CheckRange(tailFrom, boundary.Head) != ListingCheck.Passed)
            {
                return Declined(MaintenanceReason.MissingHistory, $"Commits from {tailFrom} to {boundary.Head} are not all listed");
            }
            long reads;
            try
            {
                reads = Reads(boundary, classes);
            }
            catch (OverflowException)
            {
                return Declined(MaintenanceReason.EntriesOverCap, "The planning reads overflow");
            }
            if (!context.Long && (boundary.Step == AdvanceStep.Long || reads > context.ReadsRemaining))
            {
                return new AdvancePlan() { Status = AdvancePlanStatus.NeedsLongAdmission, Detail = $"Planning needs {reads} counted reads and short admission has {context.ReadsRemaining}" };
            }

            var identity = PathCanonicalizer.Create(configuration.Store, meteredStorage);
            // Charged resolutions count as reads beside the object reads
            var resolutions = MeteredFileStorage.Of(meteredStorage) == null ? null : new ResolutionReads(context.Long ? long.MaxValue : context.ReadsRemaining - reads);
            // Before any read, when the table's last plan resolved more paths than these reads leave room for
            if (resolutions != null && boundary.Resolutions > resolutions.Left)
            {
                return new AdvancePlan()
                {
                    Status = AdvancePlanStatus.NeedsLongAdmission,
                    Detail = $"The table's last plan made {boundary.Resolutions} native path resolutions and short admission has {resolutions.Left} reads left for them"
                };
            }
            // Long admission charges as it reads, short admission once the whole plan is read
            var progressive = context.Long;
            var executor = new DeletionExecutor(configuration, meteredStorage);
            var parts = new AdvancePartWriter(context.Scratch, context.Memory, limits, c, boundary.AdvanceId);
            PathSet? protection = null;
            var planned = false;
            // Promoted for its resolutions
            var promoted = false;
            try
            {
                // Protection: live(C) adds, refs(C.json), refs(stages), then live(newest) and refs(tail), whose resolutions stay owed until all are read
                protection = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Protection);
                var protect = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
                var destination = await ReadCheckpointAsync(table, meteredStorage, boundary.Checkpoint, RecordKind.Add, protect, context);
                if (destination.Status != DestinationStatus.Read)
                {
                    return Unread(destination, c, context);
                }
                if (!destination.GapFree)
                {
                    return Declined(MaintenanceReason.HistoryChanged, $"Checkpoint {c} read from the destination is not marked gap-free");
                }
                if (protect.Declined != PathRejection.None)
                {
                    return Declined(MaintenanceReason.PathRejected, $"A protecting reference is unusable: {protect.DeclineDetail}");
                }
                await OweAsync(identity, resolutions, context, charge: false);
                if (await ReadAllAsync(meteredStorage, new[] { boundaryCommit.IOEntry }, protect, context, MaintenanceReason.HistoryChanged, identity, resolutions, charge: false) is { } declinedCommit)
                {
                    return declinedCommit;
                }
                // A stage may be published or abandoned after the listing
                if (await ReadAllAsync(meteredStorage, classes.Stages.Select(x => x.Entry).ToList(), protect, context, MaintenanceReason.Transient, identity, resolutions, charge: false) is { } declinedStage)
                {
                    return declinedStage;
                }
                DestinationRead? live = null;
                if (newest != null)
                {
                    live = await ReadCheckpointAsync(table, meteredStorage, newest, RecordKind.Add, protect, context);
                    if (live.Status != DestinationStatus.Read)
                    {
                        return Unread(live, newest.Version, context);
                    }
                    // Before the tail is read
                    if (protect.Declined != PathRejection.None)
                    {
                        return Declined(MaintenanceReason.PathRejected, $"A protecting reference is unusable: {protect.DeclineDetail}");
                    }
                    await OweAsync(identity, resolutions, context, charge: false);
                }
                var tail = new List<IOEntry>();
                for (var version = tailFrom; version <= boundary.Head; version++)
                {
                    tail.Add(listing.Commits[version].IOEntry);
                }
                if (await ReadAllAsync(meteredStorage, tail, protect, context, MaintenanceReason.HistoryChanged, identity, resolutions, charge: false) is { } declinedTail)
                {
                    return declinedTail;
                }
                await OweAsync(identity, resolutions, context, progressive);
                // Spilled once, then read again by every window
                protection.Seal(context.Stop);

                long candidates = 0;
                long protectedCount = 0;
                long kept = 0;
                long emitted = 0;
                var window = 0;
                var to = first;
                // At least one window, for tombstones(O) over an empty commit range
                do
                {
                    var from = to;
                    to = from + Math.Min(limits.WindowVersions, c - from);
                    using var set = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Candidate);
                    var collect = new CommitReferenceExtractor(identity, set, PathRole.Candidate);
                    // tombstones(O) once, in the first window
                    if (window == 0 && tombstones)
                    {
                        var root = await ReadCheckpointAsync(table, meteredStorage, boundary.OldestRoot!, RecordKind.Remove, collect, context);
                        await OweAsync(identity, resolutions, context, progressive);
                        if (root.Status != DestinationStatus.Read)
                        {
                            return Unread(root, o, context);
                        }
                    }
                    var commits = new List<IOEntry>();
                    for (var version = from; version < to; version++)
                    {
                        commits.Add(listing.Commits[version].IOEntry);
                    }
                    if (await ReadAllAsync(meteredStorage, commits, collect, context, MaintenanceReason.HistoryChanged, identity, resolutions, progressive) is { } declinedWindow)
                    {
                        return declinedWindow;
                    }
                    set.Seal(context.Stop);
                    parts.StartWindow(window);
                    using (var difference = PathSet.Difference(set, protection))
                    {
                        while (difference.MoveNext())
                        {
                            var path = difference.DeletionPathText;
                            var keep = executor.Check(executor.TargetOf(path));
                            if (keep is KeepReason.StorageChanged or KeepReason.StoreUnsupported)
                            {
                                return Declined(MaintenanceReason.CleanupGate, $"No target can be submitted: {keep}");
                            }
                            if (keep != KeepReason.None)
                            {
                                kept = checked(kept + 1);
                                continue;
                            }
                            parts.Add(path);
                            emitted = checked(emitted + 1);
                        }
                        protectedCount = checked(protectedCount + difference.Protected);
                        kept = checked(kept + difference.Untargeted);
                    }
                    parts.Flush();
                    candidates = checked(candidates + set.Count);
                    kept = checked(kept + collect.Kept);
                    window = checked(window + 1);
                }
                while (to < c);
                await ChargeAsync(identity, resolutions, context);
                planned = true;
                return new AdvancePlan()
                {
                    Status = AdvancePlanStatus.Planned,
                    Destination = destination,
                    Newest = live,
                    Parts = parts.Parts.ToList(),
                    Candidates = candidates,
                    Protected = protectedCount,
                    Kept = kept,
                    Emitted = emitted,
                    Resolutions = identity.NativeResolutions,
                    Scratch = context.Scratch
                };
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
                return Declined(e.Stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.Transient, e.Message);
            }
            catch (ResolutionsOverReadsException e)
            {
                promoted = true;
                return new AdvancePlan() { Status = AdvancePlanStatus.NeedsLongAdmission, Detail = e.Message, Resolutions = identity.NativeResolutions };
            }
            catch (CheckpointDeclinedException e)
            {
                return Declined(e.Reason, e.Message);
            }
            catch (OverflowException e)
            {
                return Declined(MaintenanceReason.EntriesOverCap, e.Message);
            }
            catch (Exception e)
            {
                return Declined(MaintenanceReason.Transient, $"{e.GetType().Name}: {e.Message}");
            }
            finally
            {
                protection?.Dispose();
                if (!planned)
                {
                    parts.DeleteParts();
                }
                parts.Dispose();
                // A decline repays its resolutions in turns while the job holds its slot
                if (!planned && !promoted)
                {
                    await identity.RepayOwedAsync(context);
                }
            }
        }

        /// <summary>
        /// The newest listed classic checkpoint above C the worker's builds did not find unsupported, when at most the checkpoint interval's commits follow it; its live adds and those commits protect as defense in depth.
        /// </summary>
        internal static LogTransactionFile? NewestAbove(AdvanceBoundary boundary) => NewestAbove(boundary.Log.Listing, boundary.C, boundary.Head, boundary.Interval, boundary.Unsupported);

        private static LogTransactionFile? NewestAbove(DeltaTransactionReader.LogListing listing, long c, long head, int interval, IReadOnlySet<long> unsupported)
        {
            var newest = listing.Checkpoints.FirstOrDefault(x => !unsupported.Contains(x.Version));
            return newest != null && newest.Version > c && head - newest.Version <= Math.Max(0, interval) ? newest : null;
        }

        /// <summary>
        /// The counted reads one plan makes: candidate commits, tombstones(O), C and C.json, stages, and the newest checkpoint above C with the commits after it.
        /// </summary>
        internal static long Reads(AdvanceBoundary boundary, LogClassification classes)
        {
            return checked(boundary.C - FirstCandidate(boundary) + Ancillary(boundary.Log.Listing, boundary.C, boundary.Head, boundary.Interval, classes.Stages.Count, Tombstones(boundary), boundary.Unsupported));
        }

        /// <summary>
        /// The counted reads besides the candidate commits of a plan to the checkpoint at c, as Reads counts them.
        /// </summary>
        internal static long Ancillary(DeltaTransactionReader.LogListing listing, long c, long head, int interval, int stages, bool tombstones, IReadOnlySet<long> unsupported)
        {
            var newest = NewestAbove(listing, c, head, interval, unsupported);
            return AdvanceBudget.Ancillary(stages, newest == null ? 0 : head - newest.Version, newest != null, tombstones);
        }

        // tombstones(O) are candidates once when O > 0 and its root is unmarked
        private static bool Tombstones(AdvanceBoundary boundary) => boundary.O > 0 && !boundary.OldestRootMarked;

        // A root at O without its own commit starts the candidate commits after it
        private static long FirstCandidate(AdvanceBoundary boundary)
        {
            var o = boundary.O;
            return boundary.OldestRoot?.Version == o && !boundary.Log.Listing.Commits.ContainsKey(o) ? o + 1 : o;
        }

        private static AdvancePlan Declined(MaintenanceReason reason, string detail) => new AdvancePlan() { Status = AdvancePlanStatus.Declined, Reason = reason, Detail = detail };

        private static AdvancePlan Unread(DestinationRead read, long version, CleanupContext context)
        {
            context.Stop.ThrowIfCancellationRequested();
            var reason = read.Status switch
            {
                DestinationStatus.Missing or DestinationStatus.Changed => MaintenanceReason.HistoryChanged,
                DestinationStatus.Invalid => MaintenanceReason.OutOfEnvelope,
                DestinationStatus.Preempted when context.Stopped == IssuanceStop.Deadline => MaintenanceReason.JobDeadline,
                _ => MaintenanceReason.Transient
            };
            return Declined(reason, $"Checkpoint {version} was not read: {read.Status}, {read.Detail}");
        }

        // Each object's references in order, their resolutions left owed; the first that cannot be used declines, with the given reason when it is gone or changed
        internal static Task<AdvancePlan?> ReadAllAsync(IFileStorage storage, IReadOnlyList<IOEntry> entries, CommitReferenceExtractor extractor, CleanupContext context,
            MaintenanceReason changed) => ReadAllAsync(storage, entries, extractor, context, changed, null, null, false);

        // With each object's resolutions charged after it
        internal static Task<AdvancePlan?> ReadAllAsync(IFileStorage storage, IReadOnlyList<IOEntry> entries, CommitReferenceExtractor extractor, CleanupContext context,
            MaintenanceReason changed, PathCanonicalizer identity) => ReadAllAsync(storage, entries, extractor, context, changed, identity, null, charge: true);

        // With each object's resolutions charged or kept owed after it, when an identity is given
        private static async Task<AdvancePlan?> ReadAllAsync(IFileStorage storage, IReadOnlyList<IOEntry> entries, CommitReferenceExtractor extractor, CleanupContext context,
            MaintenanceReason changed, PathCanonicalizer? identity, ResolutionReads? resolutions, bool charge)
        {
            // Long admission opens ahead; short admission reads one object at a time
            await using var window = new CommitReadAhead(storage, index => entries[(int)index], entries.Count, context.Long ? context.ReadAhead : 1,
                context.Transfer, () => context.CheckStop() == IssuanceStop.None, context.Stop);
            foreach (var entry in entries)
            {
                var read = await ReadCommitAsync(window, entry, extractor, identity, resolutions, charge, context);
                switch (read)
                {
                    case ObjectRead.Read:
                        continue;
                    case ObjectRead.Declined:
                        return Declined(MaintenanceReason.PathRejected, $"A protecting reference is unusable: {extractor.DeclineDetail}");
                    case ObjectRead.Spent:
                        return Declined(MaintenanceReason.Transient, $"The admission's {context.ReadLimit} reads were spent");
                    default:
                        return Declined(changed, $"A listed log object is {read} after {context.ReadsUsed} reads");
                }
            }
            return null;
        }

        // Every path one commit or stage references, as one counted read
        private static async Task<ObjectRead> ReadCommitAsync(CommitReadAhead window, IOEntry entry, CommitReferenceExtractor extractor, PathCanonicalizer? identity, ResolutionReads? resolutions,
            bool charge, CleanupContext context)
        {
            ThrowIfStopped(context);
            if (!context.TryCountRead())
            {
                return ObjectRead.Spent;
            }
            using var opened = await window.NextAsync();
            if (opened.Stopped)
            {
                throw new CleanupPreemptedException(context.CheckStop());
            }
            if (opened.Stream is not { } source)
            {
                return ObjectRead.Missing;
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
                        return ObjectRead.Declined;
                    }
                }
            }
            if (identity != null)
            {
                await OweAsync(identity, resolutions, context, charge);
            }
            return entry.Size is { } listed && listed != proof.BytesRead ? ObjectRead.Changed : ObjectRead.Read;
        }

        // The checkpoint's records of one kind, read from the destination into hashed scratch, their resolutions left owed
        private static Task<DestinationRead> ReadCheckpointAsync(IOPath table, IFileStorage storage, LogTransactionFile checkpoint, RecordKind kind, CommitReferenceExtractor extractor,
            CleanupContext context)
        {
            ThrowIfStopped(context);
            return DestinationCheckpoint.ReadAsync(table, storage, checkpoint, context.Scratch, context.Memory, context.Transfer, context, record =>
            {
                if (CheckpointRecord.Kind(record) == kind)
                {
                    extractor.AddRecord(record);
                }
            }, context.Stop);
        }

        // Owed resolutions count as reads before they are charged; a plan without the reads for them promotes
        private static async ValueTask ChargeAsync(PathCanonicalizer identity, ResolutionReads? resolutions, CleanupContext context)
        {
            if (resolutions != null)
            {
                var owed = identity.Owed;
                if (owed > resolutions.Left || !context.TryCountReads(owed))
                {
                    throw new ResolutionsOverReadsException(owed, resolutions.Left);
                }
                resolutions.Left -= owed;
            }
            await identity.ChargeOwedAsync(context.Stop, context);
        }

        // Charged now, or kept owed by a plan that promotes once they outgrow its reads
        private static ValueTask OweAsync(PathCanonicalizer identity, ResolutionReads? resolutions, CleanupContext context, bool charge)
        {
            if (charge)
            {
                return ChargeAsync(identity, resolutions, context);
            }
            if (resolutions != null && identity.Owed > resolutions.Left)
            {
                throw new ResolutionsOverReadsException(identity.Owed, resolutions.Left);
            }
            return ValueTask.CompletedTask;
        }

        private static void ThrowIfStopped(CleanupContext context)
        {
            if (context.CheckStop() is var stop && stop != IssuanceStop.None)
            {
                throw new CleanupPreemptedException(stop);
            }
        }

        // The reads a plan has beside its object reads
        private sealed class ResolutionReads
        {
            public ResolutionReads(long left)
            {
                Left = left;
            }

            public long Left { get; set; }
        }

        private sealed class ResolutionsOverReadsException : Exception
        {
            public ResolutionsOverReadsException(long owed, long left)
                : base($"Planning owes {owed} native path resolutions and short admission has {left} reads left for them")
            {
            }
        }
    }
}
