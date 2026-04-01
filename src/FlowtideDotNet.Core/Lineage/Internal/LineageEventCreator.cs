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

using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal static class LineageEventCreator
    {
        public static OpenLineageEvent CreateFromPlan(string streamName, Plan plan, IConnectorManager connectorManager)
        {
            var inputOutputFinder = new LineageInputOutputFinderVisitor(connectorManager);

            for (int i = 0; i < plan.Relations.Count; i++)
            {
                inputOutputFinder.Visit(plan.Relations[i], default!);
            }

            var inputTables = inputOutputFinder.InputTables;
            var outputTables = inputOutputFinder.OutputTables;

            LineageVisitor visitor = new LineageVisitor(plan.Relations, inputTables);
            
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                
                var relation = plan.Relations[i];

                if(relation is PlanRelation planRelation)
                {
                    relation = planRelation.Root.Input;
                }
                if (relation is RootRelation rootRel)
                {
                    relation = rootRel.Input;
                }
                if (relation is WriteRelation writeRelation)
                {
                    var lineage = visitor.HandleWriteRelation(writeRelation);
                    if (outputTables.TryGetValue(writeRelation.NamedObject.DotSeperated, out var outputTable))
                    {
                        outputTable.Facets.ColumnLineage = lineage;
                    }
                }
            }

            var run = new LineageRun(Guid.NewGuid(), new LineageRunFacets(new LineageRunProcessingEngineFacet("0.15.0", "Flowtide", "1.0.0")));
            return new OpenLineageEvent(
                DateTime.UtcNow,
                "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                LineageEventType.Start,
                run,
                new LineageJob("flowtide", streamName, new LineageJobFacets(jobType: new LineageJobTypeFacet(LineageJobProcessingType.Streaming, "flowtide", LineageJobType.Job))),
                inputTables.Values.ToList(),
                outputTables.Values.ToList()
                );
        }
    }
}
