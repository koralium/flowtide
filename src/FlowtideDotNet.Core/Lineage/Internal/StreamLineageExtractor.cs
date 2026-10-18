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

using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal static class StreamLineageExtractor
    {
        // All or nothing, connector exceptions reach the caller.
        public static StreamLineage Extract(StreamLineageExtractionContext context)
        {
            var relations = context.Plan.Relations;
            var index = new LineagePlanIndex(relations);

            var collector = new LineageTableCollector();
            for (int i = 0; i < relations.Count; i++)
            {
                if (IsRootInScope(index, i, context.SubstreamScope))
                {
                    collector.Visit(relations[i], null!);
                }
            }

            // Parallel to the writes, writes can be structurally equal.
            var upstreamReads = new List<List<ReadRelation>>(collector.Writes.Count);
            foreach (var write in collector.Writes)
            {
                var reachable = new LineageReachableReadsVisitor(index);
                reachable.Visit(write.Input, null!);
                upstreamReads.Add(reachable.Reads);
            }

            var readsByKey = new Dictionary<string, List<ReadRelation>>(StringComparer.Ordinal);
            var inputKeys = new List<string>();
            var seenReads = new HashSet<ReadRelation>(ReferenceEqualityComparer.Instance);
            AddReads(collector.Reads, readsByKey, inputKeys, seenReads);
            foreach (var reads in upstreamReads)
            {
                AddReads(reads, readsByKey, inputKeys, seenReads);
            }

            var inputTables = new Dictionary<string, LineageInputTable>(StringComparer.Ordinal);
            var inputs = new List<StreamLineageInput>(inputKeys.Count);
            foreach (var key in inputKeys)
            {
                var reads = readsByKey[key];
                var first = reads[0];
                var metadata = context.ConnectorManager.GetSourceFactory(first).GetLineageMetadata(first, context.IncludeConnectorSchema);
                inputTables.Add(key, new LineageInputTable(metadata.Namespace, metadata.TableName));
                inputs.Add(new StreamLineageInput()
                {
                    Key = key,
                    NameParts = GetNameParts(first.NamedTable.Names, metadata.TableName),
                    Namespace = metadata.Namespace,
                    TableName = metadata.TableName,
                    ConnectorColumns = LineageMerge.ToColumns(metadata.Schema),
                    PlanColumns = LineageMerge.MergeColumns(reads.Select(x => LineageMerge.ToColumns(x.BaseSchema)))
                });
            }

            var writeGroups = new Dictionary<string, List<int>>(StringComparer.Ordinal);
            var outputKeys = new List<string>();
            for (int i = 0; i < collector.Writes.Count; i++)
            {
                var key = collector.Writes[i].NamedObject.DotSeperated;
                if (!writeGroups.TryGetValue(key, out var group))
                {
                    group = new List<int>();
                    writeGroups.Add(key, group);
                    outputKeys.Add(key);
                }
                group.Add(i);
            }

            // One visitor for every write, the exchange memo is shared.
            var visitor = new LineageVisitor(index, inputTables);
            var outputs = new List<StreamLineageOutput>(outputKeys.Count);
            foreach (var key in outputKeys)
            {
                var group = writeGroups[key];
                var writes = group.Select(x => collector.Writes[x]).ToList();
                var first = writes[0];
                var metadata = context.ConnectorManager.GetSinkFactory(first).GetLineageMetadata(first, context.IncludeConnectorSchema);

                var lineages = new List<ColumnLineage>(writes.Count);
                foreach (var write in writes)
                {
                    lineages.Add(visitor.HandleWriteRelation(write));
                }

                var upstreamKeys = new List<string>();
                var seenKeys = new HashSet<string>(StringComparer.Ordinal);
                foreach (var writeIndex in group)
                {
                    foreach (var read in upstreamReads[writeIndex])
                    {
                        var readKey = read.NamedTable.DotSeperated;
                        if (inputTables.ContainsKey(readKey) && seenKeys.Add(readKey))
                        {
                            upstreamKeys.Add(readKey);
                        }
                    }
                }

                outputs.Add(new StreamLineageOutput()
                {
                    Key = key,
                    NameParts = GetNameParts(first.NamedObject.Names, metadata.TableName),
                    Namespace = metadata.Namespace,
                    TableName = metadata.TableName,
                    ConnectorColumns = LineageMerge.ToColumns(metadata.Schema),
                    PlanColumns = LineageMerge.MergeColumns(writes.Select(x => LineageMerge.ToColumns(x.TableSchema))),
                    ColumnLineage = LineageMerge.Merge(lineages),
                    UpstreamInputKeys = upstreamKeys
                });
            }

            return new StreamLineage(context.BuilderStreamName, context.SubstreamScope, context.BuildTime, inputs, outputs);
        }

        internal static IReadOnlyList<string> GetNameParts(IReadOnlyList<string> names, string tableName)
        {
            if (string.Join(".", names) == tableName)
            {
                return names.ToList();
            }

            // Shortest trailing suffix drops a catalog prefix.
            for (int count = 1; count < names.Count; count++)
            {
                var suffix = names.Skip(names.Count - count).ToList();
                if (string.Join(".", suffix) == tableName)
                {
                    return suffix;
                }
            }
            return tableName.Split('.');
        }

        // Mirrors which roots the engine builds in this substream.
        private static bool IsRootInScope(LineagePlanIndex index, int relationId, string? substreamScope)
        {
            if (substreamScope == null)
            {
                return true;
            }
            var rootSubstream = index.GetRootSubstream(relationId);
            // Globals are built in every substream.
            return rootSubstream == null || rootSubstream == substreamScope;
        }

        private static void AddReads(
            List<ReadRelation> reads,
            Dictionary<string, List<ReadRelation>> readsByKey,
            List<string> inputKeys,
            HashSet<ReadRelation> seenReads)
        {
            foreach (var read in reads)
            {
                if (!seenReads.Add(read))
                {
                    continue;
                }
                var key = read.NamedTable.DotSeperated;
                if (!readsByKey.TryGetValue(key, out var keyReads))
                {
                    keyReads = new List<ReadRelation>();
                    readsByKey.Add(key, keyReads);
                    inputKeys.Add(key);
                }
                keyReads.Add(read);
            }
        }
    }
}
