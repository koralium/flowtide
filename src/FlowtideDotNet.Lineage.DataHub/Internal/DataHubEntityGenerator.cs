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

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    internal sealed record DataHubRegistration(string StreamName, StreamLineage Lineage, long Generation);

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
        public static DataHubSnapshot Generate(IReadOnlyList<DataHubRegistration> registrations, DataHubSettings settings, DataHubCheckStatusTable checkStatuses, long version)
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
            // The inputs of one registration's writes to a table, a job can have writes from several substreams.
            var runInputs = new Dictionary<(DataHubRegistration Registration, string Output), SortedSet<string>>();
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
                    if (!runInputs.TryGetValue((scope.Registration, identity.Urn), out var inputs))
                    {
                        inputs = new SortedSet<string>(StringComparer.Ordinal);
                        runInputs.Add((scope.Registration, identity.Urn), inputs);
                    }
                    foreach (var key in output.UpstreamInputKeys)
                    {
                        if (scope.InputsByKey.TryGetValue(key, out var upstream) && upstream != null)
                        {
                            AddInput(job, inputs, upstream.Urn);
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
                                AddInput(job, inputs, upstream.Urn);
                                var upstreamField = DataHubUrns.SchemaField(upstream.Urn, datasets[upstream.Urn].FieldPath(inputField.Field));
                                job.AddEdge(downstreamField, upstreamField, inputField.Transformations);
                            }
                        }
                    }
                    foreach (var inputField in columnLineage.Dataset)
                    {
                        if (ResolveField(scope, inputField, resolution) is DataHubResolvedDataset upstream)
                        {
                            AddInput(job, inputs, upstream.Urn);
                        }
                    }
                }
            }

            var entities = new List<(DataHubEntityBuilder Builder, DataHubEntityContext Context)>();
            var jobUrnsByOutput = new Dictionary<(string Flow, string Output), string>();
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
                    jobUrnsByOutput.Add((flow.Urn, output.Urn), jobUrn);
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

            var assertions = settings.IncludeChecks ? GetAssertions(registrations, resolution) : new Dictionary<string, DataHubAssertion>(StringComparer.Ordinal);
            foreach (var assertion in assertions.Values)
            {
                var builder = assertion.Builder;
                var checkIds = assertion.Parts.Select(x => x.CheckId).Distinct().Order(StringComparer.Ordinal).ToList();
                builder.Set("assertionInfo", w => DataHubAspectWriter.WriteAssertionInfo(w, assertion.DatasetUrn, assertion.Message, assertion.StreamName, checkIds));
                builder.Set("dataPlatformInstance", w => DataHubAspectWriter.WriteDataPlatformInstance(w, DataHubUrns.Platform(DataHubUrns.Orchestrator), null));
                builder.Set("status", DataHubAspectWriter.WriteStatus);
                if (settings.AspectProvider != null)
                {
                    AddProvidedAspects(builder, new DataHubEntityContext(DataHubEntityType.Assertion, builder.Urn, assertion.StreamName, assertion.Namespace, assertion.TableName), settings.AspectProvider);
                }
                builder.Freeze();
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
            var incidents = settings.RaiseIncidents ? GetIncidents(assertions, settings) : new Dictionary<string, (DataHubAssertion, DataHubIncidentInfo)>(StringComparer.Ordinal);
            // Raised before, but no current check has them any more.
            var removedIncidents = settings.RaiseIncidents
                ? checkStatuses.GetIncidentUrns().Where(x => !incidents.ContainsKey(x)).ToHashSet(StringComparer.Ordinal)
                : new HashSet<string>(StringComparer.Ordinal);

            var runs = settings.IncludeRuns ? GetRuns(scopes, runInputs, jobUrnsByOutput, settings) : new Dictionary<string, DataHubRunInfo>(StringComparer.Ordinal);

            var urns = json.Keys.Concat(assertions.Keys).Concat(incidents.Keys).Concat(removedIncidents).Concat(runs.Keys).Order(StringComparer.Ordinal).ToList();
            return new DataHubSnapshot(urns, json, assertions, checkStatuses, version, incidents, removedIncidents, runs);
        }

        private static void AddInput(JobEntry job, SortedSet<string> runInputs, string datasetUrn)
        {
            job.AddInput(datasetUrn);
            runInputs.Add(datasetUrn);
        }

        // One run per job and registration, that is per stream or substream writing the job's table.
        // A registration that writes no table, such as a substream that only feeds the exchange, is run on the flow, else its
        // failure would be hidden while its peers wait for it. Not its stream's jobs, they change as peers register and move.
        private static Dictionary<string, DataHubRunInfo> GetRuns(
            List<RegistrationScope> scopes,
            Dictionary<(DataHubRegistration Registration, string Output), SortedSet<string>> runInputs,
            Dictionary<(string Flow, string Output), string> jobUrnsByOutput,
            DataHubSettings settings)
        {
            var runs = new Dictionary<string, DataHubRunInfo>(StringComparer.Ordinal);
            foreach (var scope in scopes)
            {
                var registration = scope.Registration;
                var substream = registration.Lineage.SubstreamName;
                var name = substream == null ? registration.StreamName : registration.StreamName + "/" + substream;
                var flowUrn = DataHubUrns.DataFlow(registration.StreamName, settings.Env);
                var outputs = GetJobOutputs(scope).Select(x => x.Identity.Urn).Distinct().ToList();
                foreach (var output in outputs)
                {
                    var jobUrn = jobUrnsByOutput[(flowUrn, output)];
                    var urn = DataHubUrns.DataProcessInstance(jobUrn, substream);
                    runs[urn] = new DataHubRunInfo(urn, name, registration.StreamName, substream, jobUrn, runInputs[(registration, output)].ToList(), [output], registration.Generation);
                }
                if (outputs.Count > 0)
                {
                    continue;
                }
                var inputs = scope.InputsByKey.Values.OfType<DataHubResolvedDataset>().Select(x => x.Urn).Distinct().Order(StringComparer.Ordinal).ToList();
                var flowRunUrn = DataHubUrns.DataProcessInstance(flowUrn, substream);
                runs[flowRunUrn] = new DataHubRunInfo(flowRunUrn, name, registration.StreamName, substream, flowUrn, inputs, [], registration.Generation);
            }
            return runs;
        }

        // Every assertion has a possible incident, served only once its check failed.
        private static Dictionary<string, (DataHubAssertion, DataHubIncidentInfo)> GetIncidents(Dictionary<string, DataHubAssertion> assertions, DataHubSettings settings)
        {
            var incidents = new Dictionary<string, (DataHubAssertion, DataHubIncidentInfo)>(StringComparer.Ordinal);
            foreach (var (assertionUrn, assertion) in assertions)
            {
                var priority = settings.IncidentPriority;
                if (settings.IncidentPriorityResolver != null)
                {
                    var context = new DataHubIncidentContext(assertion.StreamName, assertion.Message, assertionUrn, assertion.DatasetUrn, assertion.Namespace, assertion.TableName);
                    DataHubIncidentPriority? resolved;
                    try
                    {
                        resolved = settings.IncidentPriorityResolver(context);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"The DataHub incident priority resolver failed for '{assertionUrn}'.", ex);
                    }
                    if (resolved is DataHubIncidentPriority value)
                    {
                        if (!Enum.IsDefined(value))
                        {
                            throw new InvalidOperationException($"The DataHub incident priority resolver returned {value} for '{assertionUrn}', which is not a DataHubIncidentPriority.");
                        }
                        priority = value;
                    }
                }
                var info = new DataHubIncidentInfo(assertionUrn, assertion.DatasetUrn, assertion.StreamName, assertion.Message, priority);
                incidents.Add(DataHubUrns.Incident(assertionUrn), (assertion, info));
            }
            return incidents;
        }

        // One assertion per check and target dataset, the partition copies of a check share it.
        private static Dictionary<string, DataHubAssertion> GetAssertions(IReadOnlyList<DataHubRegistration> registrations, DataHubDatasetResolution resolution)
        {
            var assertions = new Dictionary<string, DataHubAssertion>(StringComparer.Ordinal);
            foreach (var registration in registrations)
            {
                // Checks with the same message on one dataset are told apart by their order.
                var ordinals = new Dictionary<(string Message, string DatasetUrn), int>();
                foreach (var check in registration.Lineage.Checks)
                {
                    foreach (var target in check.Targets)
                    {
                        if (resolution.Resolve(target.Namespace, target.TableName, target.NameParts) is not DataHubResolvedDataset dataset)
                        {
                            continue;
                        }
                        var ordinalKey = (check.Message, dataset.Urn);
                        var ordinal = ordinals.GetValueOrDefault(ordinalKey);
                        ordinals[ordinalKey] = ordinal + 1;
                        var urn = DataHubUrns.Assertion(registration.StreamName, dataset.Urn, check.Message, ordinal);
                        if (!assertions.TryGetValue(urn, out var assertion))
                        {
                            assertion = new DataHubAssertion(new DataHubEntityBuilder("assertion", urn), dataset.Urn, registration.StreamName, check.Message, target.Namespace, target.TableName);
                            assertions.Add(urn, assertion);
                        }
                        assertion.Parts.Add(new DataHubCheckPart(registration.Generation, check.CheckId, check.Replicated));
                    }
                }
            }
            return assertions;
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
                : output.Platform + "." + output.QualifiedName + "~" + output.Env + "~" + DataHubUrns.Hash(output.Urn);
            var jobUrn = DataHubUrns.DataJob(flowUrn, jobId);
            // GMS silently drops urns over 512 bytes once URL encoded.
            if (DataHubUrns.UrlEncodedLength(jobUrn) > DataHubUrns.MaxUrnLength)
            {
                jobUrn = DataHubUrns.DataJob(flowUrn, output.Platform + "~" + DataHubUrns.Hash(output.Urn));
            }
            return jobUrn;
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
