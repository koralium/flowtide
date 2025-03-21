﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Engine
{
    internal class ConnectorPlanModifyVisitor : OptimizerBaseVisitor
    {
        private readonly IConnectorManager connectorManager;

        public ConnectorPlanModifyVisitor(IConnectorManager connectorManager)
        {
            this.connectorManager = connectorManager;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            // Special case for timestamp read relation
            if (readRelation.NamedTable.DotSeperated == "__gettimestamp")
            {
                return readRelation;
            }

            var sourceFactory = connectorManager.GetSourceFactory(readRelation);
            return sourceFactory.ModifyPlan(readRelation);
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object state)
        {
            writeRelation.Input = Visit(writeRelation.Input, state);

            var sinkFactory = connectorManager.GetSinkFactory(writeRelation);
            return sinkFactory.ModifyPlan(writeRelation);
        }
    }
}
