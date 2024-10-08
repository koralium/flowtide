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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    public abstract class AggregateFunctionDefinition
    {
        public abstract Task<IAggregateContainer> CreateContainer(
            int groupingLength,
            IStateManagerClient stateManagerClient,
            IMemoryAllocator memoryAllocator,
            AggregateFunction aggregateFunction,
            ParametersInfo parametersInfo,
            ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo> visitor,
            ParameterExpression eventParameter,
            ParameterExpression stateParameter,
            ParameterExpression weightParameter,
            ParameterExpression groupingKeyParameter);
    }
}
