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

using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using System.Text;

namespace FlowtideDotNet.Core.Engine.LateMaterialization
{
    // Names the sinks that fetch for themselves.
    // That changes their state and is not in the plan's text.
    internal sealed class RowStoreRoles : OptimizerBaseVisitor
    {
        private readonly IConnectorManager _connectorManager;
        private readonly StringBuilder _text = new StringBuilder();
        private int _writes;

        private RowStoreRoles(IConnectorManager connectorManager)
        {
            _connectorManager = connectorManager;
        }

        public static string Describe(Plan plan, IConnectorManager? connectorManager)
        {
            if (connectorManager == null)
            {
                return string.Empty;
            }
            var roles = new RowStoreRoles(connectorManager);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                roles.Visit(plan.Relations[i], null!);
            }
            return roles._text.ToString();
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object state)
        {
            // Two writes to one name differ by their place
            int place = _writes++;
            var keys = _connectorManager.GetSinkFactory(writeRelation).GetRowStoreFetchKeyColumns(writeRelation);
            if (FetchesAtSink(writeRelation, keys))
            {
                _text.Append("write ").Append(place).Append(" to ").Append(writeRelation.NamedObject.DotSeperated).Append(" fetches with keys ").AppendJoin(',', keys!).Append(';');
            }
            return base.VisitWriteRelation(writeRelation, state);
        }

        // A sink fetches only with its keys as values
        public static bool FetchesAtSink(WriteRelation writeRelation, IReadOnlyList<int>? keys)
        {
            if (keys == null || keys.Count == 0 || writeRelation.EmitSet || writeRelation.Input is not FetchRequiredColumnsRelation fetch)
            {
                return false;
            }
            int inputLength = fetch.Input.OutputLength;
            for (int i = 0; i < keys.Count; i++)
            {
                if (keys[i] < 0 || keys[i] >= fetch.OutputLength)
                {
                    return false;
                }
                if ((fetch.EmitSet ? fetch.Emit[keys[i]] : keys[i]) >= inputLength)
                {
                    return false;
                }
            }
            return true;
        }
    }
}
