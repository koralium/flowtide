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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// The wide query with hosted roles, built by hand.
    /// Sources store their wide columns, the sink fetches them.
    /// </summary>
    internal static class LmHostedPlan
    {
        private sealed class ReadRewriter : OptimizerBaseVisitor
        {
            private readonly LmWorkload _workload;

            public ReadRewriter(LmWorkload workload)
            {
                _workload = workload;
            }

            public int Stores { get; private set; }

            public override Relation VisitReadRelation(ReadRelation readRelation, object state)
            {
                var table = _workload.Tables.First(x => x.Name == readRelation.NamedTable.DotSeperated);
                var names = readRelation.BaseSchema.Names;
                int reference = names.IndexOf(LmWorkload.ReferenceColumn);
                var emit = readRelation.EmitSet ? readRelation.Emit.ToList() : Enumerable.Range(0, names.Count).ToList();
                int emitted = emit.IndexOf(reference);
                if (reference < 0 || emitted < 0)
                {
                    throw new InvalidOperationException($"The read of {table.Name} does not send its reference column.");
                }
                // The reference is last, a filter's positions stay
                if (emitted != emit.Count - 1 || reference != names.Count - 1)
                {
                    throw new InvalidOperationException($"The read of {table.Name} has a shape this rewrite does not know: emit [{string.Join(",", emit)}] of [{string.Join(",", names)}], filter {(readRelation.Filter == null ? "none" : readRelation.Filter.GetType().Name)}.");
                }
                // The wide columns take the reference's place in the schema
                var newNames = names.Where(x => x != LmWorkload.ReferenceColumn).ToList();
                var types = readRelation.BaseSchema.Struct!.Types.Where((x, i) => i != reference).ToList();
                var remap = new int[names.Count];
                for (int i = 0, next = 0; i < names.Count; i++)
                {
                    remap[i] = i == reference ? -1 : next++;
                }
                var newEmit = emit.Take(emit.Count - 1).Select(x => remap[x]).ToList();
                var stored = new List<int>();
                foreach (var column in table.Columns.Where(x => x.Payload))
                {
                    stored.Add(newEmit.Count);
                    newEmit.Add(newNames.Count);
                    newNames.Add(column.Name);
                    types.Add(new StringType());
                }
                readRelation.BaseSchema = new NamedStruct() { Names = newNames, Struct = new Struct() { Types = types } };
                readRelation.Emit = newEmit;
                readRelation.RowStore = new RowStoreDefinition() { Name = table.Name, StoredColumns = stored };
                Stores++;
                return readRelation;
            }
        }

        public static Plan Build(LmWorkload workload)
        {
            if (workload.HostedSql.Length == 0)
            {
                throw new ArgumentException($"{workload.Name} has no hosted form.");
            }
            var builder = new SqlPlanBuilder();
            // Per output column: its reference's table, or null
            var output = new List<LmTable?>();
            var outputNames = new List<string>();
            foreach (var table in workload.Tables)
            {
                var names = new List<string>();
                var types = new List<SubstraitBaseType>();
                bool first = table == workload.Tables[0];
                bool referenceSent = false;
                foreach (var column in table.Columns)
                {
                    if (column.Payload)
                    {
                        if (!referenceSent)
                        {
                            referenceSent = true;
                            output.Add(table);
                            outputNames.Add(string.Empty);
                        }
                        continue;
                    }
                    names.Add(column.Name);
                    types.Add(new Int64Type());
                    if (column.Kind != LmColumnKind.Reference && (first || column.Kind != LmColumnKind.Key))
                    {
                        output.Add(null);
                        outputNames.Add(first ? column.Name : $"{table.Name}_{column.Name}");
                    }
                }
                names.Add(LmWorkload.ReferenceColumn);
                types.Add(new AnyType());
                builder.AddTableDefinition(table.Name, new NamedStruct() { Names = names, Struct = new Struct() { Types = types } });
            }
            builder.Sql(workload.HostedSql);
            var plan = PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings() { Parallelization = workload.Parallelization });

            var rewriter = new ReadRewriter(workload);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                plan.Relations[i] = rewriter.Visit(plan.Relations[i], null!);
            }
            if (rewriter.Stores != workload.Tables.Count)
            {
                throw new InvalidOperationException($"The plan reads {rewriter.Stores} tables with a store, the workload has {workload.Tables.Count}.");
            }

            // The sink takes the fetch that reads every wide column back
            var write = plan.Relations.Select(x => x is RootRelation root ? root.Input : x).OfType<WriteRelation>().Single();
            if (write.Input.OutputLength != output.Count)
            {
                throw new InvalidOperationException($"The hosted query sends {write.Input.OutputLength} columns, {output.Count} were expected.");
            }
            var fetches = new List<RowStoreFetch>();
            var emit = new List<int>();
            var writeNames = new List<string>();
            int fetched = output.Count;
            for (int i = 0; i < output.Count; i++)
            {
                var table = output[i];
                if (table == null)
                {
                    emit.Add(i);
                    writeNames.Add(outputNames[i]);
                    continue;
                }
                var payload = table.Columns.Where(x => x.Payload).ToList();
                fetches.Add(new RowStoreFetch() { ReferenceColumn = i, StoreName = table.Name, Columns = Enumerable.Range(0, payload.Count).ToList() });
                for (int c = 0; c < payload.Count; c++)
                {
                    emit.Add(fetched++);
                    writeNames.Add(table == workload.Tables[0] ? payload[c].Name : $"{table.Name}_{payload[c].Name}");
                }
            }
            write.Input = new FetchRequiredColumnsRelation() { Input = write.Input, Fetches = fetches, Emit = emit };
            write.TableSchema = new NamedStruct() { Names = writeNames };
            return plan;
        }
    }
}
