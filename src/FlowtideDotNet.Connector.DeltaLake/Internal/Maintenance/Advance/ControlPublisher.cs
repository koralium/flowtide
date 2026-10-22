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
using Stowage;
using System.Security.Cryptography;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// A control record read back under its exact name: its length and whole SHA-256.
    /// </summary>
    internal sealed record PublishedRecord(string Name, long Length, byte[] Sha256);

    /// <summary>
    /// Publishes an advance's control records one at a time, each attempt under a fresh name and read back.
    /// </summary>
    internal sealed class ControlPublisher
    {
        public const int MaxAttempts = 3;

        private readonly IFileStorage _metered;
        private readonly IOPath _log;
        private readonly CleanupContext _context;
        private readonly StowageCheckpointTransport _transport;
        private readonly Func<string> _tokens;
        private readonly Func<string, long>? _freeSpace;
        private readonly Action<string>? _faultForTests;
        private readonly Func<ControlKind, string, Task>? _beforePublish;

        // The hook is awaited before each attempt with its record's kind and name
        public ControlPublisher(IFileStorage metered, IOPath table, CleanupContext context, Func<string>? tokens = null, Func<string, long>? freeSpace = null, Action<string>? faultForTests = null,
            Func<ControlKind, string, Task>? beforePublish = null)
        {
            _metered = metered;
            _log = table.Combine(DeltaTransactionReader.DeltaLogDirName);
            _context = context;
            _transport = new StowageCheckpointTransport(metered, table);
            _tokens = tokens ?? ControlTokens.New;
            _freeSpace = freeSpace;
            _faultForTests = faultForTests;
            _beforePublish = beforePublish;
        }

        /// <summary>
        /// Publishes the plan's parts in ordinal order, then the header naming each part as it was read back; returns the header.
        /// </summary>
        public async Task<PublishedRecord> PublishManifestAsync(AdvanceBoundary boundary, AdvancePlan plan)
        {
            var anchor = boundary.Anchor ?? throw new ArgumentException("The boundary carries no anchor", nameof(boundary));
            var destination = plan.Destination?.Proof ?? throw new ArgumentException("The plan carries no destination proof", nameof(plan));
            var descriptors = new List<PartDescriptor>(plan.Parts.Count);
            long partBytes = 0;
            foreach (var part in plan.Parts)
            {
                var (_, attemptId) = await AttemptAsync(ControlKind.Part, attempt => ControlName.Part(boundary.C, boundary.AdvanceId, part.Ordinal, attempt), part.ScratchPath, part.Bytes, part.Sha256);
                descriptors.Add(new PartDescriptor(part.Ordinal, attemptId, checked((int)part.Paths), checked((int)part.Bytes), part.Sha256));
                partBytes = checked(partBytes + part.Bytes);
            }
            var header = new HeaderRecord(boundary.C, boundary.AdvanceId, boundary.TableId, boundary.O, boundary.A, boundary.Head, boundary.FirstJson, boundary.Cutoff.UtcTicks,
                AnchorSummary.Of(anchor), CheckpointSummary.Of(destination), plan.Candidates, plan.Protected, plan.Kept, new RecordTotals(plan.Emitted, descriptors.Count, partBytes));
            var buffer = _context.Memory.Allocate(ControlRecords.HeaderLength(header, descriptors.Count));
            try
            {
                var length = ControlRecords.WriteHeader(buffer, header, descriptors, out var sha256);
                return await PublishBytesAsync(ControlKind.Header, attempt => ControlName.Header(boundary.C, boundary.AdvanceId, attempt), buffer.AsMemory(0, length), sha256);
            }
            finally
            {
                _context.Memory.Free(buffer);
            }
        }

        /// <summary>
        /// Publishes one started, logs-complete, progress or done record of the header's group.
        /// </summary>
        public Task<PublishedRecord> PublishAsync(ControlKind kind, Association association, long sequence, Func<byte[]> encode)
        {
            var bytes = encode();
            return PublishBytesAsync(kind, attempt => ControlName.Group(kind, association.C, association.AdvanceId, association.HeaderSha256, attempt, sequence), bytes, SHA256.HashData(bytes));
        }

        // Every attempt publishes from one scratch file
        private async Task<PublishedRecord> PublishBytesAsync(ControlKind kind, Func<string, string> name, ReadOnlyMemory<byte> bytes, byte[] sha256)
        {
            var scratch = _context.Scratch;
            var local = scratch.NewPath(".record");
            try
            {
                using (var stream = scratch.Create(local))
                {
                    stream.Write(bytes.Span);
                }
                return (await AttemptAsync(kind, name, local, bytes.Length, sha256)).Record;
            }
            finally
            {
                scratch.Delete(local);
            }
        }

        // Up to MaxAttempts attempts, each under a fresh attempt id
        private async Task<(PublishedRecord Record, string AttemptId)> AttemptAsync(ControlKind kind, Func<string, string> name, string local, long length, byte[] sha256)
        {
            string? failure = null;
            for (int attempt = 0; attempt < MaxAttempts; attempt++)
            {
                var attemptId = _tokens();
                var recordName = name(attemptId);
                var destination = _log.Combine(recordName);
                try
                {
                    if (_beforePublish != null)
                    {
                        await _beforePublish(kind, recordName);
                    }
                    await IssueAsync(kind, local, length, destination);
                    using var read = await ControlRecords.ReadAsync(_metered, destination, kind, _context.Memory, _context.Stop);
                    if (Confirms(kind, recordName, read, length, sha256))
                    {
                        return (new PublishedRecord(recordName, length, sha256), attemptId);
                    }
                    failure = $"{recordName} read back {read.Status} with {read.Length} bytes";
                }
                catch (Exception e) when (e is not (CleanupPreemptedException or CheckpointDeclinedException) && (e is not OperationCanceledException || !_context.Stop.IsCancellationRequested))
                {
                    failure = $"{e.GetType().Name}: {e.Message}";
                }
            }
            throw new CheckpointDeclinedException(MaintenanceReason.PublishFailed, DeclineDependency.None, $"A {kind} record was not published in {MaxAttempts} attempts: {failure}");
        }

        // One issued publication, run off the context's lock; a progress record of settled deletes passes a due build and the deadline
        private async Task IssueAsync(ControlKind kind, string local, long length, IOPath destination)
        {
            Func<CancellationToken, Task> operation = stop => Task.Run(async () =>
            {
                if (_transport.NativePath(destination) != null)
                {
                    // The copy and the rename
                    await _transport.ChargeNativeAsync(2, length, stop);
                }
                await CheckpointPublisher.PublishFileAsync(_transport, _context.Scratch, _context.Memory, local, length, destination, overwrite: false, _freeSpace, _faultForTests, stop);
            }, stop);
            var issued = kind == ControlKind.Progress ? _context.TryIssueSettled(operation) : _context.TryIssue(operation);
            if (issued == null)
            {
                throw new CleanupPreemptedException(_context.Stopped);
            }
            await issued;
        }

        // The bytes written, accepted by their kind's decoder and agreeing with the exact name
        private static bool Confirms(ControlKind kind, string name, RecordBytes read, long length, byte[] sha256)
        {
            if (read.Status != RecordStatus.Valid || read.Length != length || !read.Sha256.AsSpan().SequenceEqual(sha256))
            {
                return false;
            }
            var bytes = read.Bytes.Span;
            var decoded = kind switch
            {
                ControlKind.Part => ControlRecords.ValidatePart(bytes, out _),
                ControlKind.Header => ControlRecords.DecodeHeader(bytes, out _, null),
                ControlKind.Started => ControlRecords.Decode(bytes, out StartedRecord? _),
                ControlKind.LogsComplete => ControlRecords.Decode(bytes, out LogsCompleteRecord? _),
                ControlKind.Progress => ControlRecords.Decode(bytes, out ProgressRecord? _),
                _ => ControlRecords.Decode(bytes, out DoneRecord? _)
            };
            return decoded == RecordStatus.Valid && ControlName.TryParse(name, out var parsed) && ControlRecords.Matches(parsed, bytes) == RecordStatus.Valid;
        }
    }
}
