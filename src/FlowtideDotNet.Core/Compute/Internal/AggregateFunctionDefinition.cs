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

using FlexBuffers;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal class AggregateFunctionDefinition
    {
        public AggregateFunctionDefinition(
            string uri,
            string name,
            Func<AggregateFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> updateStateFunc,
            Func<byte[], FlxValue> stateToValueFunc)
        {
            Uri = uri;
            Name = name;
            UpdateStateFunc = updateStateFunc;
            StateToValueFunc = stateToValueFunc;
        }

        public string Uri { get; }

        public string Name { get; }

        public Func<byte[], FlxValue> StateToValueFunc { get; }

        /// <summary>
        /// Function that updates the state of the aggregate function
        /// Arguments:
        /// - AggregateFunction
        /// - ParametersInfo used by the expression visitor
        /// - ExpressionVisitor, visitor to visit function arguments
        /// - ParameterExpression, byte[] parameter for the state
        /// - ParameterExpression, int, parameter for the weight
        /// Should return a byte[] with the new state.
        /// </summary>
        public Func<AggregateFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> UpdateStateFunc { get; }
    }
}
