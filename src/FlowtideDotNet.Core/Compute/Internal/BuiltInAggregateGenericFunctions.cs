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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInAggregateGenericFunctions
    {
        public static void AddBuiltInAggregateGenericFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStreamingAggregateFunction(
                FunctionsAggregateGeneric.Uri,
                FunctionsAggregateGeneric.Count,
                (aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter) =>
                {
                    if (aggregateFunction.Arguments != null && aggregateFunction.Arguments.Count != 0)
                    {
                        throw new ArgumentException("Count function does not take any arguments");
                    }
                    var expr = GetCountBody();
                    var body = expr.Body;
                    var replacer = new ParameterReplacerVisitor(expr.Parameters[0], parametersInfo.Parameters[0]);
                    Expression e = replacer.Visit(body);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
                    e = replacer.Visit(e);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
                    e = replacer.Visit(e);
                    return e;
                },
                GetCountValue
                );
        }

        private static FlxValue GetCountValue(byte[]? state)
        {
            if (state == null)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(0));
            }
            var count = BinaryPrimitives.ReadInt64LittleEndian(state);
            return FlxValue.FromBytes(FlexBuffer.SingleValue(count));
        }

        private static Expression<Func<RowEvent, byte[], long, byte[]>> GetCountBody()
        {
            return (ev, bytes, weight) => DoCount(ev, bytes, weight);
        }

        private static byte[] DoCount(RowEvent streamEvent, byte[] currentState, long weight)
        {
            if (currentState == null)
            {
                currentState = new byte[8];
            }
            var currentCount = BinaryPrimitives.ReadInt64LittleEndian(currentState);
            var newCount = currentCount + weight;
            BinaryPrimitives.WriteInt64LittleEndian(currentState, newCount);
            return currentState;
        }
    }
}
