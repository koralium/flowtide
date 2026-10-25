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

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal static class LineageEventCreator
    {
        public static OpenLineageEvent CreateFromLineage(Guid runId, StreamLineage lineage, bool includeSchema)
        {
            // Fresh tables per event, column lineage is immutable.
            var inputTables = new List<LineageInputTable>(lineage.Inputs.Count);
            foreach (var input in lineage.Inputs)
            {
                var inputTable = new LineageInputTable(input.Namespace, input.TableName);
                if (includeSchema)
                {
                    inputTable.Facets.Schema = LineageSchemaConverter.ConvertToFacet(input.SchemaColumns);
                }
                inputTables.Add(inputTable);
            }

            var outputTables = new List<LineageOutputTable>(lineage.Outputs.Count);
            foreach (var output in lineage.Outputs)
            {
                var outputTable = new LineageOutputTable(output.Namespace, output.TableName);
                outputTable.Facets.ColumnLineage = output.ColumnLineage;
                if (includeSchema)
                {
                    outputTable.Facets.Schema = LineageSchemaConverter.ConvertToFacet(output.SchemaColumns);
                }
                outputTables.Add(outputTable);
            }

            var run = new LineageRun(runId, new LineageRunFacets(new LineageRunProcessingEngineFacet(OpenLineageConstants.EngineVersion, OpenLineageConstants.EngineName)));
            return new OpenLineageEvent(
                DateTime.UtcNow,
                OpenLineageConstants.Producer,
                LineageEventType.Start,
                run,
                new LineageJob(OpenLineageConstants.JobNamespace, lineage.BuilderStreamName, new LineageJobFacets(jobType: new LineageJobTypeFacet(LineageJobProcessingType.Streaming, OpenLineageConstants.Integration, LineageJobType.Job))),
                inputTables,
                outputTables
                );
        }
    }
}
