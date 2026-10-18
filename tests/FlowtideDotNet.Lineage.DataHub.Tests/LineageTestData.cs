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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Lineage.DataHub.Tests
{
    internal static class LineageTestData
    {
        public static readonly DateTimeOffset BuildTime = new DateTimeOffset(2026, 10, 2, 12, 0, 0, TimeSpan.Zero);

        internal const string WindowSql = @"
            CREATE TABLE input1 (a int, b int, c string);
            CREATE TABLE output (c1 int, c2 int);
            INSERT INTO output
            SELECT SUM(a) OVER (PARTITION BY b ORDER BY c) as c1, b as c2 FROM input1;
            ";

        public static LineageColumn Col(string name, SubstraitBaseType? type = null)
        {
            return new LineageColumn(name, type ?? AnyType.Instance);
        }

        public static StreamLineageInput Input(string ns, string table, IReadOnlyList<LineageColumn> columns, IReadOnlyList<LineageColumn>? connectorColumns = null)
        {
            return new StreamLineageInput()
            {
                Key = table,
                NameParts = table.Split('.'),
                Namespace = ns,
                TableName = table,
                ConnectorColumns = connectorColumns,
                PlanColumns = columns
            };
        }

        public static StreamLineageOutput Output(
            string ns,
            string table,
            IReadOnlyList<LineageColumn> columns,
            Dictionary<string, LineageInputField[]> fields,
            LineageInputField[]? dataset = null,
            string[]? upstream = null,
            IReadOnlyList<LineageColumn>? connectorColumns = null)
        {
            return new StreamLineageOutput()
            {
                Key = table,
                NameParts = table.Split('.'),
                Namespace = ns,
                TableName = table,
                ConnectorColumns = connectorColumns,
                PlanColumns = columns,
                ColumnLineage = new ColumnLineage(
                    fields.ToDictionary(x => x.Key, x => new ColumnLineageField(x.Value)),
                    dataset ?? []),
                UpstreamInputKeys = upstream ?? []
            };
        }

        public static StreamLineage Snapshot(IReadOnlyList<StreamLineageInput> inputs, IReadOnlyList<StreamLineageOutput> outputs, string? substream = null)
        {
            return new StreamLineage("builder", substream, BuildTime, inputs, outputs);
        }

        public static LineageInputField Field(string ns, string table, string field, LineageTransformationType type, LineageTransformationSubtype subtype)
        {
            return new LineageInputField(ns, table, field, [new LineageTransformation(type, subtype)]);
        }

        public static LineageInputField Identity(string ns, string table, string field)
        {
            return Field(ns, table, field, LineageTransformationType.Direct, LineageTransformationSubtype.Identity);
        }

        public static LineageInputField Indirect(string ns, string table, string field, LineageTransformationSubtype subtype)
        {
            return Field(ns, table, field, LineageTransformationType.Indirect, subtype);
        }

        // Real extraction through the SQL planner and optimizer.
        public static StreamLineage ExtractWithSql(string sql, string ns)
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(sql);
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new LineageOnlySourceFactory(ns));
            connectorManager.AddSink(new LineageOnlySinkFactory(ns));
            return StreamLineageExtractor.Extract(new StreamLineageExtractionContext()
            {
                Plan = PlanOptimizer.Optimize(sqlPlanBuilder.GetPlan()),
                ConnectorManager = connectorManager,
                BuilderStreamName = "builder",
                IncludeConnectorSchema = true,
                BuildTime = BuildTime
            });
        }

        // Reports lineage only, building a stream with it fails.
        private sealed class LineageOnlySourceFactory : AbstractConnectorSourceFactory
        {
            private readonly string _namespace;

            public LineageOnlySourceFactory(string @namespace)
            {
                _namespace = @namespace;
            }

            public override bool CanHandle(ReadRelation readRelation)
            {
                return !readRelation.NamedTable.DotSeperated.StartsWith("__", StringComparison.Ordinal);
            }

            public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            {
                throw new NotSupportedException();
            }

            public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            {
                return new TableLineageMetadata(_namespace, readRelation.NamedTable.DotSeperated, null);
            }
        }

        private sealed class LineageOnlySinkFactory : AbstractConnectorSinkFactory
        {
            private readonly string _namespace;

            public LineageOnlySinkFactory(string @namespace)
            {
                _namespace = @namespace;
            }

            public override bool CanHandle(WriteRelation writeRelation)
            {
                return true;
            }

            public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            {
                throw new NotSupportedException();
            }

            public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            {
                return new TableLineageMetadata(_namespace, writeRelation.NamedObject.DotSeperated, null);
            }
        }
    }
}
