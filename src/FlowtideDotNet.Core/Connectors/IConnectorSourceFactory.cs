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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Connectors
{
    /// <summary>
    /// Factory for creating source connectors.
    /// First call handle is called, if the factory responds true, then ModifyPlan is called.
    /// After that, CanHandle is called again, if it returns true, then CreateSource is called.
    /// 
    /// This is done since the plan may be modified to include multiple read relations in case of partitions.
    /// So each new read relation will have to be called with CanHandle and CreateSource.
    /// </summary>
    public interface IConnectorSourceFactory
    {
        bool CanHandle(ReadRelation readRelation);

        /// <summary>
        /// Allows for modification of the plan before it is executed.
        /// This is useful for adding steps before the read relation.
        /// </summary>
        /// <param name="readRelation"></param>
        /// <returns></returns>
        Relation ModifyPlan(ReadRelation readRelation);

        /// <summary>
        /// Create the source vertex for the read relation.
        /// </summary>
        /// <param name="readRelation"></param>
        /// <param name="functionsRegister"></param>
        /// <returns></returns>
        IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions);
    }
}
