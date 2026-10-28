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

using Apache.Arrow;
using Apache.Arrow.Memory;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using Stowage;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    // Claims a candidate file's copies of the batch's rows
    internal sealed class FileMatcher
    {
        private readonly IFileStorage _storage;
        private readonly IOPath _table;
        private readonly string _tableName;
        private readonly StructType _schema;
        private readonly bool _columnsById;
        private readonly IArrowArray[] _columns;
        // Hits of one Arrow batch, in row order
        private readonly List<(long Position, int Entry)> _hits = new List<(long Position, int Entry)>();
        private ulong[] _hashes = System.Array.Empty<ulong>();
        private bool[] _rejected = System.Array.Empty<bool>();

        public FileMatcher(IFileStorage storage, IOPath table, string tableName, StructType schema, bool columnsById)
        {
            _storage = storage;
            _table = table;
            _tableName = tableName;
            _schema = schema;
            _columnsById = columnsById;
            _columns = new IArrowArray[schema.Fields.Count];
        }

        public async Task Scan(int id, DeleteBatch batch, DeleteRun run)
        {
            var statistics = run.Statistics;
            var started = Stopwatch.GetTimestamp();
            using var file = await ParquetSharpReader.OpenDataFile(_storage, _table, run.Records[id].Path, forDeletes: true);
            var binding = FileBinding.Create(_schema, file, _tableName, _columnsById);
            // Claimed positions and raw rows both follow the footer
            file.CheckRowGroups(binding.LeafColumns);
            run.FooterRows[id] = file.NumRows;
            statistics.FilesScanned++;
            statistics.OpenTicks += Stopwatch.GetTimestamp() - started;

            using var precision = new RowGroupFlags(file, binding, statistics);
            long position = 0;
            await using var batches = file.ReadBatches(binding.LeafColumns).GetAsyncEnumerator();
            while (batch.Unresolved > 0)
            {
                started = Stopwatch.GetTimestamp();
                var read = await batches.MoveNextAsync();
                statistics.ReadTicks += Stopwatch.GetTimestamp() - started;
                if (!read)
                {
                    break;
                }
                using (var source = batches.Current)
                using (var rebound = binding.Rebind(source, MemoryAllocator.Default.Value))
                {
                    started = Stopwatch.GetTimestamp();
                    Collect(rebound, position, batch, statistics);
                    statistics.HashTicks += Stopwatch.GetTimestamp() - started;
                    position += rebound.Length;
                }
                if (_hits.Count == 0)
                {
                    continue;
                }

                await run.Load(id);
                foreach (var (hit, entry) in _hits)
                {
                    if (batch.Remaining(entry) == 0 || run.IsDeleted(id, hit) || precision.IsInexact(hit))
                    {
                        continue;
                    }
                    run.Claim(id, hit);
                    batch.Claim(entry);
                }
            }
        }

        private void EnsureBuffers(int length)
        {
            if (_hashes.Length < length)
            {
                _hashes = new ulong[length];
                _rejected = new bool[length];
            }
        }

        private void Collect(ReboundBatch rebound, long position, DeleteBatch batch, DeleteStatistics statistics)
        {
            _hits.Clear();
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i] = rebound.Column(i);
            }
            EnsureBuffers(rebound.Length);
            var hashes = _hashes.AsSpan(0, rebound.Length);
            var rejected = _rejected.AsSpan(0, rebound.Length);
            var prefilter = batch.PrefilterColumn >= 0;
            if (prefilter)
            {
                batch.HashPrefilter(_columns[batch.PrefilterColumn], hashes, rejected);
            }
            else
            {
                batch.HashBatch(_columns, hashes, rejected);
            }
            var skipped = 0;
            var hashed = 0;
            for (int row = 0; row < rebound.Length; row++)
            {
                if (rejected[row])
                {
                    skipped++;
                    continue;
                }
                var hash = hashes[row];
                // Only a row past the first column's test gets the full hash
                if (prefilter && !batch.MayMatch(hash))
                {
                    continue;
                }
                if (prefilter && !batch.TryHash(_columns, row, out hash))
                {
                    skipped++;
                    continue;
                }
                hashed++;
                var entry = batch.Find(hash, _columns, row);
                if (entry >= 0 && batch.Remaining(entry) > 0)
                {
                    _hits.Add((position + row, entry));
                }
            }
            statistics.RowsScanned += rebound.Length;
            statistics.RowsSkipped += skipped;
            statistics.RowsHashed += hashed;
            statistics.Hits += _hits.Count;
        }

        // The current row group's raw leaves, read forward to each hit
        private sealed class RowGroupFlags : IDisposable
        {
            private readonly ParquetDataFile _file;
            private readonly FileBinding _binding;
            private readonly DeleteStatistics _statistics;
            private readonly bool _repeated;
            private int _group = -1;
            private TimestampPrecision.RowGroupCursor? _cursor;

            public RowGroupFlags(ParquetDataFile file, FileBinding binding, DeleteStatistics statistics)
            {
                _file = file;
                _binding = binding;
                _statistics = statistics;
                _repeated = binding.PrecisionLeaves.Any(x => x.MaxRepetitionLevel > 0);
            }

            // Positions only move forward within a file
            public bool IsInexact(long position)
            {
                if (_binding.PrecisionLeaves.Count == 0)
                {
                    return false;
                }
                var started = Stopwatch.GetTimestamp();
                var starts = _file.RowGroupStarts;
                if (_group < 0 || (_group + 1 < starts.Length && starts[_group + 1] <= position))
                {
                    var group = Math.Max(_group, 0);
                    while (group + 1 < starts.Length && starts[group + 1] <= position)
                    {
                        group++;
                    }
                    // Passed groups' repeated leaves are read to count their rows
                    if (_repeated)
                    {
                        _cursor?.Finish();
                        for (int passed = _group + 1; passed < group; passed++)
                        {
                            using var cursor = new TimestampPrecision.RowGroupCursor(_file.LowLevel, _file.Path, passed, _binding.PrecisionLeaves);
                            cursor.Finish();
                            _statistics.RowGroupsRawRead++;
                            _statistics.RawLevelsRead += cursor.LevelsRead;
                        }
                    }
                    Close();
                    _cursor = new TimestampPrecision.RowGroupCursor(_file.LowLevel, _file.Path, group, _binding.PrecisionLeaves);
                    _statistics.RowGroupsRawRead++;
                    _group = group;
                }
                var flagged = _cursor!.Flagged(position - starts[_group]);
                _statistics.PrecisionTicks += Stopwatch.GetTimestamp() - started;
                return flagged;
            }

            public void Dispose()
            {
                Close();
            }

            private void Close()
            {
                if (_cursor != null)
                {
                    _statistics.RawLevelsRead += _cursor.LevelsRead;
                    _cursor.Dispose();
                    _cursor = null;
                }
            }
        }
    }
}
