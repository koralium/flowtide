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
using FlowtideDotNet.Core.Compute.Internal.StatefulAggregations;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Buffers.Binary;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInArithmaticFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());
        public static void AddBuiltInArithmaticFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Add, (x, y) => AddImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Subtract, (x, y) => SubtractImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Multiply, (x, y) => MultiplyImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Divide, (x, y) => DivideImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Negate, (x) => NegateImplementation(x));

            // Aggregate functions
            functionsRegister.RegisterStreamingAggregateFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum, 
                (aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter) =>
                {
                    if (aggregateFunction.Arguments.Count != 1)
                    {
                        throw new InvalidOperationException("Sum must have one argument.");
                    }
                    var arg = visitor.Visit(aggregateFunction.Arguments[0], parametersInfo);
                    var expr = GetSumBody();
                    var body = expr.Body;
                    var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
                    Expression e = replacer.Visit(body);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
                    e = replacer.Visit(e);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
                    e = replacer.Visit(e);
                    return e;
                }, GetSumValue);

            functionsRegister.RegisterStreamingAggregateFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum0,
                (aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter) =>
                {
                    if (aggregateFunction.Arguments.Count != 1)
                    {
                        throw new InvalidOperationException("Sum must have one argument.");
                    }
                    var arg = visitor.Visit(aggregateFunction.Arguments[0], parametersInfo);
                    var expr = GetSumBody();
                    var body = expr.Body;
                    var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
                    Expression e = replacer.Visit(body);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
                    e = replacer.Visit(e);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
                    e = replacer.Visit(e);
                    return e;
                }, GetSum0Value);

            MinMaxAggregationRegistration.RegisterMin(functionsRegister);
            MinMaxAggregationRegistration.RegisterMax(functionsRegister);
        }

        private static FlxValue AddImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble + y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble + y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong + y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong + y.AsDouble));
                }
            }
            return NullValue;
        }

        private static FlxValue SubtractImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble - y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble - y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong - y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong - y.AsDouble));
                }
            }
            return NullValue;
        }

        private static FlxValue MultiplyImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble * y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble * y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong * y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong * y.AsDouble));
                }
            }
            return NullValue;
        }

        private static FlxValue DivideImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble / y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble / y.AsLong));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return a double when dividing
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong / (double)y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong / y.AsDouble));
                }
            }
            return NullValue;
        }

        private static FlxValue NegateImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(-x.AsDouble));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(-x.AsLong));
            }
            return NullValue;
        }

        private static FlxValue GetSumValue(byte[]? state)
        {
            if (state == null)
            {
                return NullValue;
            }
            var sum = BinaryPrimitives.ReadDoubleLittleEndian(state);
            return FlxValue.FromBytes(FlexBuffer.SingleValue(sum));
        }

        private static FlxValue GetSum0Value(byte[]? state)
        {
            if (state == null)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(0.0));
            }
            var sum = BinaryPrimitives.ReadDoubleLittleEndian(state);
            return FlxValue.FromBytes(FlexBuffer.SingleValue(sum));
        }

        private static Expression<Func<FlxValue, byte[], long, byte[]>> GetSumBody()
        {
            return (ev, bytes, weight) => DoSum(ev, bytes, weight);
        }

        private static byte[] DoSum(FlxValue column, byte[] currentState, long weight)
        {
            if (currentState == null)
            {
                currentState = new byte[8];
            }
            var currentSum = BinaryPrimitives.ReadDoubleLittleEndian(currentState);
            if (column.ValueType == FlexBuffers.Type.Int)
            {
                currentSum += (column.AsLong * weight);
            }
            else if (column.ValueType == FlexBuffers.Type.Float)
            {
                currentSum += (column.AsDouble * weight);
            }

            BinaryPrimitives.WriteDoubleLittleEndian(currentState, currentSum);
            return currentState;
        }
    }
}
