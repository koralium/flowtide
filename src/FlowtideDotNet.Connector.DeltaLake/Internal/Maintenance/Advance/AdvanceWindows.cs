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

using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// The bounds of windowed planning: commit versions per window, paths and bytes per part, and parts per plan.
    /// </summary>
    internal sealed record AdvanceWindowLimits
    {
        public static AdvanceWindowLimits Default { get; } = new AdvanceWindowLimits();

        public int WindowVersions { get; init; } = 20_000;

        public int PartPaths { get; init; } = 10_000;

        // The whole part record
        public int PartBytes { get; init; } = 2 << 20;

        // The descriptors one header may hold
        public int MaxParts { get; init; } = 16_384;
    }

    /// <summary>
    /// Cuts a plan's emitted deletion paths into bounded part records of one advance in job scratch, each sorted by UTF-8 bytes.
    /// </summary>
    internal sealed class AdvancePartWriter : IDisposable
    {
        // Charged per buffered path beside its bytes
        private const int EntryOverhead = 32;

        // A part record's bytes besides its paths and their length prefixes
        private static readonly int s_partOverhead = ControlRecords.PartLength(0, 0);

        private readonly JobScratch _scratch;
        private readonly JobMemory _memory;
        private readonly AdvanceWindowLimits _limits;
        private readonly long _c;
        private readonly string _advanceId;
        private readonly List<byte[]> _pending = new List<byte[]>();
        private readonly List<AdvancePart> _parts = new List<AdvancePart>();
        // Path bytes plus a length prefix each
        private int _pendingBytes;
        private long _charged;
        private int _window;
        private int _index;

        public AdvancePartWriter(JobScratch scratch, JobMemory memory, AdvanceWindowLimits limits, long c, string advanceId)
        {
            _scratch = scratch;
            _memory = memory;
            _limits = limits;
            _c = c;
            _advanceId = advanceId;
        }

        public IReadOnlyList<AdvancePart> Parts => _parts;

        /// <summary>
        /// Writes what the previous window left and numbers the next parts from zero.
        /// </summary>
        public void StartWindow(int window)
        {
            Flush();
            _window = window;
            _index = 0;
        }

        /// <summary>
        /// Buffers one deletion path; a path no part record can hold declines.
        /// </summary>
        public void Add(string deletionPath)
        {
            var length = Encoding.UTF8.GetByteCount(deletionPath);
            if (length > ControlRecords.MaxPathBytes || ControlRecords.PartLength(length, 1) > _limits.PartBytes)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.EntriesOverCap, DeclineDependency.None, $"A deletion path of {length} bytes fits no part of {_limits.PartBytes} bytes");
            }
            if (_pending.Count >= _limits.PartPaths || s_partOverhead + _pendingBytes + length + 4 > _limits.PartBytes)
            {
                Flush();
            }
            _memory.Reserve(length + EntryOverhead);
            _charged += length + EntryOverhead;
            var bytes = new byte[length];
            Encoding.UTF8.GetBytes(deletionPath, bytes);
            _pending.Add(bytes);
            _pendingBytes += length + 4;
        }

        /// <summary>
        /// Writes the buffered paths as one part record; a plan over its descriptor bound declines.
        /// </summary>
        public void Flush()
        {
            if (_pending.Count == 0)
            {
                return;
            }
            if (_parts.Count >= _limits.MaxParts)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.EntriesOverCap, DeclineDependency.None, $"The plan needs more than {_limits.MaxParts} parts");
            }
            _pending.Sort(static (left, right) => left.AsSpan().SequenceCompareTo(right));
            var buffer = _memory.Allocate(s_partOverhead + _pendingBytes);
            try
            {
                var ordinal = _parts.Count;
                var length = ControlRecords.WritePart(buffer, _c, _advanceId, ordinal, _pending, out var sha256);
                var path = _scratch.NewPath(".part");
                try
                {
                    using var stream = _scratch.Create(path);
                    stream.Write(buffer, 0, length);
                }
                catch
                {
                    _scratch.Delete(path);
                    throw;
                }
                _parts.Add(new AdvancePart(_window, _index++, path, _pending.Count, length, sha256) { Ordinal = ordinal });
            }
            finally
            {
                _memory.Free(buffer);
                ReleasePending();
            }
        }

        // Parts of a plan that is not returned
        public void DeleteParts()
        {
            foreach (var part in _parts)
            {
                _scratch.Delete(part.ScratchPath);
            }
            _parts.Clear();
        }

        private void ReleasePending()
        {
            _pending.Clear();
            _pendingBytes = 0;
            _memory.Release(_charged);
            _charged = 0;
        }

        public void Dispose()
        {
            ReleasePending();
        }
    }
}
