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

using FlexBuffers;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using static Substrait.Protobuf.AggregateRel.Types;

namespace FlowtideDotNet.Core.Operators.Aggregate
{
    internal static class MeasureCompiler
    {
        public static (Func<StreamEvent, byte[], long, byte[]>, Func<byte[], FlxValue>) CompileMeasure(AggregateFunction aggregateFunction, FunctionsRegister functionsRegister)
        {
            if (functionsRegister.TryGetAggregateFunction(aggregateFunction.ExtensionUri, aggregateFunction.ExtensionName, out var definition))
            {
                var param = System.Linq.Expressions.Expression.Parameter(typeof(StreamEvent));
                var stateParam = System.Linq.Expressions.Expression.Parameter(typeof(byte[]));
                var weightParam = System.Linq.Expressions.Expression.Parameter(typeof(long));

                var parametersInfo = new ParametersInfo(new List<ParameterExpression>() { param }, new List<int> { 0 });
                var expressionVisitor = new FlowtideExpressionVisitor(functionsRegister, typeof(StreamEvent));
                var expr = definition.UpdateStateFunc(aggregateFunction, parametersInfo, expressionVisitor, stateParam, weightParam);
                var lambda = System.Linq.Expressions.Expression.Lambda<Func<StreamEvent, byte[], long, byte[]>>(expr, param, stateParam, weightParam);

                return (lambda.Compile(), definition.StateToValueFunc);
            }
            else
            {
                throw new InvalidOperationException($"The function {aggregateFunction.ExtensionUri}:{aggregateFunction.ExtensionName} is not defined.");
            }
        }
    }
}
