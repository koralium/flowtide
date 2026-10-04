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

using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Type;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    internal sealed record DataHubRegistration(string StreamName, StreamLineage Lineage);

    // One data flow per stream, one data job per stream and output dataset.
    internal static class DataHubEntityGenerator
    {
        private sealed class RegistrationScope
        {
            public RegistrationScope(DataHubRegistration registration)
            {
                Registration = registration;
            }

            public DataHubRegistration Registration { get; }

            public Dictionary<string, DataHubResolvedDataset?> InputsByKey { get; } = new Dictionary<string, DataHubResolvedDataset?>(StringComparer.Ordinal);

            public Dictionary<(string, string), DataHubResolvedDataset?> InputsByName { get; } = new Dictionary<(string, string), DataHubResolvedDataset?>();

            public List<DataHubResolvedDataset?> Outputs { get; } = new List<DataHubResolvedDataset?>();
        }

        private sealed class DatasetEntry
        {
            public DatasetEntry(DataHubResolvedDataset identity)
            {
                Identity = identity;
            }

            public DataHubResolvedDataset Identity { get; }

            public List<LineageMergedColumn> WrittenColumns { get; } = new List<LineageMergedColumn>();

            public List<LineageMergedColumn> ReadColumns { get; } = new List<LineageMergedColumn>();

            public List<LineageMergedColumn> ReferencedColumns { get; } = new List<LineageMergedColumn>();

            public LineageColumnSet Columns { get; private set; } = new LineageColumnSet();

            // Written columns first, then reads, then referenced fields.
            public void FinishColumns()
            {
                var columns = new LineageColumnSet();
                columns.AddRange(WrittenColumns);
                columns.AddRange(ReadColumns);
                columns.AddRange(ReferencedColumns);
                Columns = columns;
            }

            public string FieldPath(string column)
            {
                var name = Columns.Canonical(column);
                return Identity.LowercaseColumns ? name.ToLowerInvariant() : name;
            }
        }

        private sealed class FlowEntry
        {
            public FlowEntry(string streamName, string urn)
            {
                StreamName = streamName;
                Urn = urn;
            }

            public string StreamName { get; }

            public string Urn { get; }

            // Keyed by output dataset urn.
            public SortedDictionary<string, JobEntry> Jobs { get; } = new SortedDictionary<string, JobEntry>(StringComparer.Ordinal);
        }

        private sealed class JobEntry
        {
            private readonly HashSet<string> _inputSet = new HashSet<string>(StringComparer.Ordinal);

            public JobEntry(DataHubResolvedDataset output)
            {
                Output = output;
            }

            public DataHubResolvedDataset Output { get; }

            // First seen order.
            public List<string> Inputs { get; } = new List<string>();

            // Keyed by downstream field path.
            public Dictionary<string, FieldEdges> Fields { get; } = new Dictionary<string, FieldEdges>(StringComparer.Ordinal);

            public void AddInput(string datasetUrn)
            {
                if (_inputSet.Add(datasetUrn))
                {
                    Inputs.Add(datasetUrn);
                }
            }

            public void AddEdge(string downstreamField, string upstreamFieldUrn, IReadOnlyList<LineageTransformation> transformations)
            {
                if (!Fields.TryGetValue(downstreamField, out var edges))
                {
                    edges = new FieldEdges();
                    Fields.Add(downstreamField, edges);
                }
                edges.Upstreams.Add(upstreamFieldUrn);
                foreach (var transformation in transformations)
                {
                    edges.Transformations.Add(FormatTransformation(transformation));
                }
            }
        }

        private sealed class FieldEdges
        {
            public SortedSet<string> Upstreams { get; } = new SortedSet<string>(StringComparer.Ordinal);

            public SortedSet<string> Transformations { get; } = new SortedSet<string>(StringComparer.Ordinal);
        }

        // Registrations arrive in (stream, substream) ordinal order.
        public static DataHubSnapshot Generate(IReadOnlyList<DataHubRegistration> registrations, DataHubSettings settings)
        {
            var resolution = new DataHubDatasetResolution(settings);
            var datasets = new Dictionary<string, DatasetEntry>(StringComparer.Ordinal);
            var scopes = new List<RegistrationScope>(registrations.Count);

            foreach (var registration in registrations)
            {
                var scope = new RegistrationScope(registration);
                foreach (var input in registration.Lineage.Inputs)
                {
                    var identity = resolution.Resolve(input.Namespace, input.TableName, input.NameParts);
                    scope.InputsByKey.TryAdd(input.Key, identity);
                    scope.InputsByName.TryAdd((input.Namespace, input.TableName), identity);
                    if (identity != null)
                    {
                        AddColumns(GetDataset(datasets, identity).ReadColumns, input, settings.IncludeConnectorSchema);
                    }
                }
                foreach (var output in registration.Lineage.Outputs)
                {
                    var identity = resolution.Resolve(output.Namespace, output.TableName, output.NameParts);
                    scope.Outputs.Add(identity);
                    if (identity != null)
                    {
                        var dataset = GetDataset(datasets, identity);
                        AddColumns(dataset.WrittenColumns, output, settings.IncludeConnectorSchema);
                        // Every projected alias needs a schema field.
                        foreach (var key in output.ColumnLineage?.Fields.Keys.Order(StringComparer.Ordinal) ?? Enumerable.Empty<string>())
                        {
                            dataset.WrittenColumns.Add(new LineageMergedColumn(key, AnyType.Instance, false));
                        }
                    }
                }
                scopes.Add(scope);
            }

            // Fields a job reads must exist upstream.
            foreach (var scope in scopes)
            {
                foreach (var (output, _) in GetJobOutputs(scope))
                {
                    foreach (var field in AllInputFields(output))
                    {
                        if (ResolveField(scope, field, resolution) is DataHubResolvedDataset identity)
                        {
                            GetDataset(datasets, identity).ReferencedColumns.Add(new LineageMergedColumn(field.Field, AnyType.Instance, false));
                        }
                    }
                }
            }

            foreach (var dataset in datasets.Values)
            {
                dataset.FinishColumns();
            }

            // Keyed by urn, stream names that encode alike share one flow.
            var flows = new SortedDictionary<string, FlowEntry>(StringComparer.Ordinal);
            foreach (var scope in scopes)
            {
                var streamName = scope.Registration.StreamName;
                var flowUrn = DataHubUrns.DataFlow(streamName, settings.Env);
                if (!flows.TryGetValue(flowUrn, out var flow))
                {
                    flow = new FlowEntry(streamName, flowUrn);
                    flows.Add(flowUrn, flow);
                }
                foreach (var (output, identity) in GetJobOutputs(scope))
                {
                    if (!flow.Jobs.TryGetValue(identity.Urn, out var job))
                    {
                        job = new JobEntry(identity);
                        flow.Jobs.Add(identity.Urn, job);
                    }
                    foreach (var key in output.UpstreamInputKeys)
                    {
                        if (scope.InputsByKey.TryGetValue(key, out var upstream) && upstream != null)
                        {
                            job.AddInput(upstream.Urn);
                        }
                    }
                    if (output.ColumnLineage is not ColumnLineage columnLineage)
                    {
                        continue;
                    }
                    var target = datasets[identity.Urn];
                    foreach (var field in columnLineage.Fields)
                    {
                        var downstreamField = target.FieldPath(field.Key);
                        foreach (var inputField in field.Value.InputFields)
                        {
                            if (ResolveField(scope, inputField, resolution) is DataHubResolvedDataset upstream)
                            {
                                job.AddInput(upstream.Urn);
                                var upstreamField = DataHubUrns.SchemaField(upstream.Urn, datasets[upstream.Urn].FieldPath(inputField.Field));
                                job.AddEdge(downstreamField, upstreamField, inputField.Transformations);
                            }
                        }
                    }
                    foreach (var inputField in columnLineage.Dataset)
                    {
                        if (ResolveField(scope, inputField, resolution) is DataHubResolvedDataset upstream)
                        {
                            job.AddInput(upstream.Urn);
                        }
                    }
                }
            }

            var entities = new List<(DataHubEntityBuilder Builder, DataHubEntityContext Context)>();
            foreach (var flow in flows.Values)
            {
                var flowEntity = new DataHubEntityBuilder("dataFlow", flow.Urn);
                flowEntity.Set("dataFlowInfo", w => DataHubAspectWriter.WriteDataFlowInfo(w, flow.StreamName, settings.Env));
                flowEntity.Set("status", DataHubAspectWriter.WriteStatus);
                entities.Add((flowEntity, new DataHubEntityContext(DataHubEntityType.DataFlow, flow.Urn, flow.StreamName, null, null)));

                var jobUrns = GetJobUrns(flow, settings.Env);
                foreach (var job in flow.Jobs.Values)
                {
                    var output = job.Output;
                    var jobUrn = jobUrns[output.Urn];
                    var fineGrainedLineages = GetFineGrainedLineages(job, datasets[output.Urn]);
                    var jobEntity = new DataHubEntityBuilder("dataJob", jobUrn);
                    jobEntity.Set("dataJobInfo", w => DataHubAspectWriter.WriteDataJobInfo(w, output.QualifiedName, flow.Urn, settings.Env, output.Namespace, output.TableName));
                    jobEntity.Set("dataJobInputOutput", w => DataHubAspectWriter.WriteDataJobInputOutput(w, job.Inputs, output.Urn, fineGrainedLineages));
                    jobEntity.Set("status", DataHubAspectWriter.WriteStatus);
                    entities.Add((jobEntity, new DataHubEntityContext(DataHubEntityType.DataJob, jobUrn, flow.StreamName, output.Namespace, output.TableName)));
                }
            }

            foreach (var dataset in datasets.Values)
            {
                var identity = dataset.Identity;
                var datasetEntity = new DataHubEntityBuilder("dataset", identity.Urn);
                if (identity.IncludeMetadata)
                {
                    var platformUrn = DataHubUrns.Platform(identity.Platform);
                    datasetEntity.Set("status", DataHubAspectWriter.WriteStatus);
                    // An empty schema would replace the real one in DataHub.
                    if (dataset.Columns.Columns.Count > 0)
                    {
                        var fields = dataset.Columns.Columns.Select(x => new DataHubSchemaField(dataset.FieldPath(x.Name), x.Type)).ToList();
                        datasetEntity.Set("schemaMetadata", w => DataHubAspectWriter.WriteSchemaMetadata(w, identity.QualifiedName, platformUrn, fields));
                    }
                    if (identity.PlatformInstance != null)
                    {
                        var instanceUrn = DataHubUrns.PlatformInstance(identity.Platform, identity.PlatformInstance);
                        datasetEntity.Set("dataPlatformInstance", w => DataHubAspectWriter.WriteDataPlatformInstance(w, platformUrn, instanceUrn));
                    }
                }
                entities.Add((datasetEntity, new DataHubEntityContext(DataHubEntityType.Dataset, identity.Urn, null, identity.Namespace, identity.TableName)));
            }

            var json = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            // Only the own platform, a built-in platform would lose its DataHub logo and name.
            if (settings.IncludePlatformInfo && flows.Count > 0)
            {
                var platform = new DataHubEntityBuilder("dataPlatform", DataHubUrns.Platform(DataHubUrns.Orchestrator));
                platform.Set("dataPlatformInfo", w => DataHubAspectWriter.WriteDataPlatformInfo(w, DataHubUrns.Orchestrator, "Flowtide", settings.PlatformLogoUrl));
                json.Add(platform.Urn, platform.ToUtf8Json());
            }
            foreach (var (builder, context) in entities)
            {
                if (settings.AspectProvider != null)
                {
                    AddProvidedAspects(builder, context, settings.AspectProvider);
                }
                // Datasets without metadata are served only for provided aspects.
                if (builder.AspectCount > 0)
                {
                    json.Add(builder.Urn, builder.ToUtf8Json());
                }
            }
            var urns = json.Keys.Order(StringComparer.Ordinal).ToList();
            return new DataHubSnapshot(urns, json);
        }

        private static void AddProvidedAspects(DataHubEntityBuilder builder, DataHubEntityContext context, Func<DataHubEntityContext, IEnumerable<DataHubAspect>?> provider)
        {
            try
            {
                foreach (var aspect in provider(context) ?? [])
                {
                    var value = aspect.Value;
                    builder.Set(aspect.Name, w => value.WriteTo(w));
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"The DataHub aspect provider failed for '{context.Urn}'.", ex);
            }
        }

        // Keyed by output dataset urn.
        private static Dictionary<string, string> GetJobUrns(FlowEntry flow, string flowEnv)
        {
            var jobUrns = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var output in flow.Jobs.Values.Select(x => x.Output))
            {
                jobUrns.Add(output.Urn, GetJobUrn(flow.Urn, output, flowEnv));
            }
            return jobUrns;
        }

        // From the output alone: plain ids have no '~' and split at the first '.', all others end in '~' and a hash of the output urn.
        private static string GetJobUrn(string flowUrn, DataHubResolvedDataset output, string flowEnv)
        {
            var plain = string.Equals(output.Env, flowEnv, StringComparison.Ordinal) &&
                !output.Platform.Contains('.') &&
                !output.Platform.Contains('~') &&
                !output.QualifiedName.Contains('~');
            var jobId = plain
                ? output.Platform + "." + output.QualifiedName
                : output.Platform + "." + output.QualifiedName + "~" + output.Env + "~" + UrnHash(output.Urn);
            var jobUrn = DataHubUrns.DataJob(flowUrn, jobId);
            // GMS silently drops urns over 512 bytes once URL encoded.
            if (DataHubUrns.UrlEncodedLength(jobUrn) > DataHubUrns.MaxUrnLength)
            {
                jobUrn = DataHubUrns.DataJob(flowUrn, output.Platform + "~" + UrnHash(output.Urn));
            }
            return jobUrn;
        }

        // 128 bits, so distinct outputs never share a hash.
        private static string UrnHash(string urn)
        {
            return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(urn)), 0, 16).ToLowerInvariant();
        }

        // Output column order, unknown fields last.
        private static List<DataHubFineGrainedLineage> GetFineGrainedLineages(JobEntry job, DatasetEntry target)
        {
            var positions = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var column in target.Columns.Columns)
            {
                positions.TryAdd(target.FieldPath(column.Name), positions.Count);
            }
            return job.Fields
                .OrderBy(x => positions.TryGetValue(x.Key, out var position) ? position : int.MaxValue)
                .ThenBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => new DataHubFineGrainedLineage(
                    x.Value.Upstreams.ToList(),
                    DataHubUrns.SchemaField(job.Output.Urn, x.Key),
                    x.Value.Transformations.Count == 0 ? null : string.Join(",", x.Value.Transformations)))
                .ToList();
        }

        // Same TYPE:SUBTYPE text as DataHub's OpenLineage converter.
        private static string FormatTransformation(LineageTransformation transformation)
        {
            var type = transformation.Type == LineageTransformationType.Direct ? "DIRECT" : "INDIRECT";
            var subtype = transformation.SubType switch
            {
                LineageTransformationSubtype.Identity => "IDENTITY",
                LineageTransformationSubtype.Transformation => "TRANSFORMATION",
                LineageTransformationSubtype.Aggregation => "AGGREGATION",
                LineageTransformationSubtype.Join => "JOIN",
                LineageTransformationSubtype.GroupBy => "GROUP_BY",
                LineageTransformationSubtype.Filter => "FILTER",
                LineageTransformationSubtype.Sort => "SORT",
                LineageTransformationSubtype.Window => "WINDOW",
                LineageTransformationSubtype.Conditional => "CONDITIONAL",
                _ => "UNKNOWN"
            };
            return type + ":" + subtype;
        }

        private static IEnumerable<(StreamLineageOutput Output, DataHubResolvedDataset Identity)> GetJobOutputs(RegistrationScope scope)
        {
            var outputs = scope.Registration.Lineage.Outputs;
            for (int i = 0; i < outputs.Count; i++)
            {
                if (scope.Outputs[i] is DataHubResolvedDataset identity)
                {
                    yield return (outputs[i], identity);
                }
            }
        }

        private static IEnumerable<LineageInputField> AllInputFields(StreamLineageOutput output)
        {
            if (output.ColumnLineage == null)
            {
                return [];
            }
            return output.ColumnLineage.Fields.Values.SelectMany(x => x.InputFields).Concat(output.ColumnLineage.Dataset);
        }

        private static DataHubResolvedDataset? ResolveField(RegistrationScope scope, LineageInputField field, DataHubDatasetResolution resolution)
        {
            if (scope.InputsByName.TryGetValue((field.Namespace, field.TableName), out var identity))
            {
                return identity;
            }
            return resolution.Resolve(field.Namespace, field.TableName, field.TableName.Split('.'));
        }

        // Connector columns only when this store asked for them.
        private static void AddColumns(List<LineageMergedColumn> target, StreamLineageTable table, bool includeConnectorSchema)
        {
            if (includeConnectorSchema && table.ConnectorColumns != null)
            {
                foreach (var column in table.ConnectorColumns)
                {
                    target.Add(new LineageMergedColumn(column.Name, column.Type, true));
                }
            }
            foreach (var column in table.PlanColumns)
            {
                target.Add(new LineageMergedColumn(column.Name, column.Type, false));
            }
        }

        private static DatasetEntry GetDataset(Dictionary<string, DatasetEntry> datasets, DataHubResolvedDataset identity)
        {
            if (!datasets.TryGetValue(identity.Urn, out var dataset))
            {
                dataset = new DatasetEntry(identity);
                datasets.Add(identity.Urn, dataset);
            }
            return dataset;
        }
    }
}
