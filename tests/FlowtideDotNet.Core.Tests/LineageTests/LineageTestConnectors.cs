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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    internal class LineageTestSourceFactory : AbstractConnectorSourceFactory
    {
        private readonly Func<ReadRelation, NamedStruct?>? _schema;
        private readonly string _namespace;

        public LineageTestSourceFactory(Func<ReadRelation, NamedStruct?>? schema = null, string @namespace = "test_ns")
        {
            _schema = schema;
            _namespace = @namespace;
        }

        // Internal reads stay unclaimed, as in production.
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
            return new TableLineageMetadata(_namespace, readRelation.NamedTable.DotSeperated, includeSchema ? _schema?.Invoke(readRelation) : null);
        }
    }

    internal class LineageTestSinkFactory : AbstractConnectorSinkFactory
    {
        private readonly Func<WriteRelation, NamedStruct?>? _schema;
        private readonly string _namespace;

        public LineageTestSinkFactory(Func<WriteRelation, NamedStruct?>? schema = null, string @namespace = "test_ns")
        {
            _schema = schema;
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
            return new TableLineageMetadata(_namespace, writeRelation.NamedObject.DotSeperated, includeSchema ? _schema?.Invoke(writeRelation) : null);
        }
    }

    internal static class LineageTestHelper
    {
        public static Plan BuildPlan(string sql, PlanOptimizerSettings? settings = null)
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(sql);
            return PlanOptimizer.Optimize(sqlPlanBuilder.GetPlan(), settings);
        }

        public static ConnectorManager CreateConnectorManager(Func<ReadRelation, NamedStruct?>? sourceSchema = null)
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new LineageTestSourceFactory(sourceSchema));
            connectorManager.AddSink(new LineageTestSinkFactory());
            return connectorManager;
        }

        public static StreamLineage ExtractWithSql(string sql, PlanOptimizerSettings? settings = null, string? scope = null)
        {
            return Extract(BuildPlan(sql, settings), CreateConnectorManager(), scope);
        }

        public static StreamLineage Extract(Plan plan, IConnectorManager connectorManager, string? scope = null, bool includeSchema = false)
        {
            return StreamLineageExtractor.Extract(new StreamLineageExtractionContext()
            {
                Plan = plan,
                ConnectorManager = connectorManager,
                BuilderStreamName = "s",
                SubstreamScope = scope,
                IncludeConnectorSchema = includeSchema,
                BuildTime = DateTimeOffset.UnixEpoch
            });
        }

        public static void AssertLineageEquivalent(ColumnLineage expected, ColumnLineage? actual)
        {
            Assert.NotNull(actual);
            Assert.Equal(expected.Fields.Keys.Order(StringComparer.Ordinal), actual.Fields.Keys.Order(StringComparer.Ordinal));
            foreach (var field in expected.Fields)
            {
                Assert.Equal(Describe(field.Value.InputFields), Describe(actual.Fields[field.Key].InputFields));
            }
            Assert.Equal(Describe(expected.Dataset), Describe(actual.Dataset));
        }

        // Order free view of input fields.
        private static List<string> Describe(IReadOnlyList<LineageInputField> fields)
        {
            return fields
                .Select(x => $"{x.Namespace}|{x.TableName}|{x.Field}|{string.Join(",", x.Transformations.Select(t => $"{t.Type}/{t.SubType}").Order(StringComparer.Ordinal))}")
                .Order(StringComparer.Ordinal)
                .ToList();
        }
    }
}
