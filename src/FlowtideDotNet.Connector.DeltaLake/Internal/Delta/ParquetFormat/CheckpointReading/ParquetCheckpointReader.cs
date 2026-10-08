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
using ParquetSharp.Arrow;
using Stowage;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading
{
    internal class ParquetCheckpointReader
    {
        // commitInfo, cdc, rowIdHighWaterMark and checkpointMetadata are not part of the snapshot
        private static readonly HashSet<string> s_snapshotColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "add", "remove", "metaData", "protocol", "txn", "domainMetadata", "sidecar"
        };

        // Parsed copies of fields that are also stored as text
        private static readonly HashSet<string> s_parsedCopies = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "stats_parsed", "partitionValues_parsed"
        };

        // Kept from stats_parsed, a file with a deletion vector must have a record count even without JSON stats
        private const string RecordCountField = "numRecords";

        /// <summary>
        /// Streams the snapshot actions of a classic checkpoint, throws <see cref="CheckpointUnusableException"/> when it uses sidecars.
        /// </summary>
        public async IAsyncEnumerable<DeltaAction> ReadCheckpointFile(IFileStorage storage, IOEntry path, bool includeRemoves = true, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var stream = await storage.OpenRead(path.Path);

            if (stream == null)
            {
                throw new FileNotFoundException($"File not found: {path.Path}");
            }

            using ParquetSharp.Arrow.FileReader fileReader = new ParquetSharp.Arrow.FileReader(stream);

            var columns = SnapshotColumns(fileReader.SchemaManifest, includeRemoves);
            if (columns.Length == 0)
            {
                yield break;
            }

            using var batchReader = fileReader.GetRecordBatchReader(null, columns);

            CheckpointReadVisitor? visitor = null;
            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync(cancellationToken)) != null)
            {
                using (batch)
                {
                    // Row order keeps the actions in file order
                    for (int i = 0; i < batch.Length; i++)
                    {
                        for (int c = 0; c < batch.ColumnCount; c++)
                        {
                            var field = batch.Schema.FieldsList[c];
                            var col = batch.Column(c);

                            if (col.IsNull(i))
                            {
                                continue;
                            }
                            if (field.Name.Equals("sidecar", StringComparison.OrdinalIgnoreCase))
                            {
                                throw new CheckpointUnusableException($"Checkpoint {path.Path} keeps file actions in sidecar files, which are not supported.");
                            }

                            if (visitor == null)
                            {
                                visitor = new CheckpointReadVisitor(field, i);
                            }
                            else
                            {
                                visitor.Reset(field, i);
                            }

                            var action = visitor.GetAction(col);
                            if (action != null)
                            {
                                yield return action;
                            }
                        }
                    }
                }
            }
        }

        private static int[] SnapshotColumns(SchemaManifest manifest, bool includeRemoves)
        {
            var columns = new List<int>();
            foreach (var field in manifest.SchemaFields)
            {
                // Checkpoint removes are only tombstones, their keys never match a checkpoint add
                if (!includeRemoves && field.Field.Name.Equals("remove", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                if (s_snapshotColumns.Contains(field.Field.Name))
                {
                    AddLeaves(field, columns, skipParsedCopies: true);
                }
            }
            return columns.ToArray();
        }

        private static void AddLeaves(SchemaField field, List<int> columns, bool skipParsedCopies)
        {
            if (field.ColumnIndex >= 0)
            {
                columns.Add(field.ColumnIndex);
                return;
            }
            foreach (var child in field.Children)
            {
                if (skipParsedCopies && s_parsedCopies.Contains(child.Field.Name))
                {
                    if (child.Field.Name.Equals("stats_parsed", StringComparison.OrdinalIgnoreCase))
                    {
                        foreach (var statistic in child.Children)
                        {
                            if (statistic.Field.Name.Equals(RecordCountField, StringComparison.OrdinalIgnoreCase) && statistic.ColumnIndex >= 0)
                            {
                                columns.Add(statistic.ColumnIndex);
                            }
                        }
                    }
                    continue;
                }
                AddLeaves(child, columns, skipParsedCopies: false);
            }
        }
    }
}
