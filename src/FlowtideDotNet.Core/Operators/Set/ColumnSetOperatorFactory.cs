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

using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Core.Operators.Set.Structs;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Set
{
    internal class ColumnSetOperatorFactory
    {
        public static MultipleInputVertex<StreamEventBatch, SetOperatorState> CreateColumnSetOperator(SetRelation setRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            if (setRelation.Operation == SetOperation.UnionAll)
            {
                return new UnionAllSetOperator(setRelation, executionDataflowBlockOptions);
            }

            switch(setRelation.Inputs.Count)
            {
                case 1:
                    return new ColumnSetOperator<InputWeights1>(setRelation, executionDataflowBlockOptions);
                case 2:
                    return new ColumnSetOperator<InputWeights2>(setRelation, executionDataflowBlockOptions);
                case 3:
                    return new ColumnSetOperator<InputWeights3>(setRelation, executionDataflowBlockOptions);
                case 4:
                    return new ColumnSetOperator<InputWeights4>(setRelation, executionDataflowBlockOptions);
                case 5:
                    return new ColumnSetOperator<InputWeights5>(setRelation, executionDataflowBlockOptions);
                case 6:
                    return new ColumnSetOperator<InputWeights6>(setRelation, executionDataflowBlockOptions);
                case 7:
                    return new ColumnSetOperator<InputWeights7>(setRelation, executionDataflowBlockOptions);
                case 8:
                    return new ColumnSetOperator<InputWeights8>(setRelation, executionDataflowBlockOptions);
                default:
                    throw new NotSupportedException($"Input count {setRelation.Inputs.Count} is not supported, only 8 inputs are supported at this time.");
            }
        }
    }
}
