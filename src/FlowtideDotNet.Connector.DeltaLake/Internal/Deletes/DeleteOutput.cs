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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using Stowage;
using System.Numerics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    /// <summary>
    /// Writes each file's new deletes as a remove, a deletion vector or a rewrite.
    /// </summary>
    internal sealed class DeleteOutput
    {
        private readonly IFileStorage _storage;
        private readonly IOPath _table;
        private readonly string _tableName;
        private readonly StructType _schema;
        private readonly bool _columnsById;
        private readonly bool _deletionVectors;
        private readonly ParquetSharp.WriterProperties? _writerProperties;
        private readonly Func<ParquetSharpWriter, bool> _isFull;
        private readonly Func<ParquetSharpWriter, Task> _writeFile;

        public DeleteOutput(IFileStorage storage, IOPath table, string tableName, StructType schema, bool columnsById, bool deletionVectors, ParquetSharp.WriterProperties? writerProperties, Func<ParquetSharpWriter, bool> isFull, Func<ParquetSharpWriter, Task> writeFile)
        {
            _storage = storage;
            _table = table;
            _tableName = tableName;
            _schema = schema;
            _columnsById = columnsById;
            _deletionVectors = deletionVectors;
            _writerProperties = writerProperties;
            _isFull = isFull;
            _writeFile = writeFile;
        }

        // Only files with a new claim have a vector, walked in id order
        public async Task WriteAsync(Dictionary<int, ModifiableDeleteVector> deletes, Dictionary<int, DeltaFileRecord> files, Dictionary<int, long> footerRows, List<DeltaAction> actions, long currentTime, CatalogOverlay overlay, List<string> created)
        {
            foreach (var (id, deleted) in deletes.OrderBy(x => x.Key))
            {
                var file = files[id];
                var rows = footerRows[id];
                var count = CountPositions(file.Path, deleted, rows);
                actions.Add(new DeltaAction()
                {
                    Remove = new DeltaRemoveFileAction()
                    {
                        Path = file.Path,
                        DeletionVector = file.DeletionVector,
                        DataChange = true,
                        DeletionTimestamp = currentTime,
                        Stats = file.Statistics,
                        Size = file.Size,
                        PartitionValues = file.PartitionValues
                    }
                });

                if (count == rows)
                {
                    overlay.Removes.Add(id);
                    continue;
                }
                if (_deletionVectors && (double)count / rows < 0.1)
                {
                    await WriteDeletionVector(id, file, deleted, rows, actions, overlay, created);
                    continue;
                }

                using var source = await ParquetSharpReader.OpenDataFile(_storage, _table, file.Path, forDeletes: true);
                var binding = FileBinding.Create(_schema, source, _tableName, _columnsById);
                if (HasInexactSurvivor(source, binding, deleted))
                {
                    if (!_deletionVectors)
                    {
                        throw new InvalidOperationException($"Delta table {_tableName} cannot rewrite {file.Path}: a surviving row holds a timestamp finer than a microsecond. Enable deletion vectors on the table to delete from this file");
                    }
                    await WriteDeletionVector(id, file, deleted, rows, actions, overlay, created);
                    continue;
                }

                var writer = ParquetSharpWriter.ForRewrite(_schema, binding, _writerProperties);
                writer.NewBatch();
                await writer.CopyFrom(source, binding, deleted, _isFull, () => _writeFile(writer));
                if (writer.WrittenCount > 0)
                {
                    await _writeFile(writer);
                }
                overlay.Removes.Add(id);
            }
        }

        // Ascending positions inside the footer's rows, as many as the vector counts
        private static long CountPositions(string path, IDeleteVector deleted, long rows)
        {
            long count = 0;
            long last = -1;
            foreach (var position in deleted)
            {
                if (position < 0 || position >= rows)
                {
                    throw new InvalidDataException($"Delta file {path} cannot be decoded: deleted row {position} is outside its {rows} rows");
                }
                if (position <= last)
                {
                    throw new InvalidDataException($"Delta file {path} cannot be decoded: deleted row {position} comes twice or out of order");
                }
                last = position;
                count++;
            }
            if (count != deleted.Cardinality)
            {
                throw new InvalidDataException($"Delta file {path} cannot be decoded: its deletion vector counts {deleted.Cardinality} rows and holds {count}");
            }
            return count;
        }

        private async Task WriteDeletionVector(int id, DeltaFileRecord file, ModifiableDeleteVector deleted, long rows, List<DeltaAction> actions, CatalogOverlay overlay, List<string> created)
        {
            var roaringBitmap = deleted.ToRoaringBitmapArray();
            var (deletePath, z85string) = DeletionVectorWriter.GenerateDestination();

            // Owned before the write, a failed write may still leave the file
            created.Add(deletePath);
            var (_, dataSize) = await DeletionVectorWriter.WriteDeletionVector(_storage, _table, deletePath, roaringBitmap);

            var reAdded = file.ToAdd().WithDeletionVector(new DeletionVector()
            {
                Cardinality = roaringBitmap.Cardinality,
                Offset = 1,
                StorageType = "u",
                PathOrInlineDv = z85string,
                SizeInBytes = dataSize
            }, rows);
            actions.Add(new DeltaAction()
            {
                Add = reAdded
            });
            // Same file and bounds, the record takes the footer's count
            overlay.Updates.Add((id, DeltaFileRecord.FromAdd(reAdded, rows)));
        }

        // A survivor holding a timestamp a microsecond copy would change
        private static bool HasInexactSurvivor(ParquetDataFile source, FileBinding binding, IDeleteVector deleted)
        {
            if (binding.PrecisionLeaves.Count == 0)
            {
                return false;
            }
            for (int group = 0; group < source.RowGroupStarts.Length; group++)
            {
                var flags = TimestampPrecision.FlagRowGroup(source.LowLevel, source.Path, group, binding.PrecisionLeaves);
                if (flags == null)
                {
                    continue;
                }
                var start = source.RowGroupStarts[group];
                for (int word = 0; word < flags.Length; word++)
                {
                    for (var bits = flags[word]; bits != 0; bits &= bits - 1)
                    {
                        if (!deleted.Contains(start + word * 64L + BitOperations.TrailingZeroCount(bits)))
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }
}
