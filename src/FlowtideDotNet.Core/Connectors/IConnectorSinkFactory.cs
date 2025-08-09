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

using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Connectors
{
    public interface IConnectorSinkFactory
    {
        bool CanHandle(WriteRelation writeRelation);

        /// <summary>
        /// Allows for modification of the plan before it is executed.
        /// This is useful for adding steps before the read relation.
        /// </summary>
        /// <param name="readRelation"></param>
        /// <returns></returns>
        Relation ModifyPlan(WriteRelation writeRelation);

        /// <summary>
        /// Create the source vertex for the read relation.
        /// </summary>
        /// <param name="readRelation"></param>
        /// <param name="functionsRegister"></param>
        /// <returns></returns>
        IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions);
    }
}
