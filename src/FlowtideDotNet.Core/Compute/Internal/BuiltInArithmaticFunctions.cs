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
        private static FlxValue NaNValue = FlxValue.FromBytes(FlexBuffer.SingleValue(double.NaN));

        public static void AddBuiltInArithmaticFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Add, (x, y) => AddImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Subtract, (x, y) => SubtractImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Multiply, (x, y) => MultiplyImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Divide, (x, y) => DivideImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Negate, (x) => NegateImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Modulo, (x, y) => ModuloImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Power, (x, y) => PowerImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Sqrt, (x) => SqrtImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Exp, (x) => ExpImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Cos, (x) => CosImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Sin, (x) => SinImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Tan, (x) => TanImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Cosh, (x) => CoshImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Sinh, (x) => SinhImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Tanh, (x) => TanhImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Acos, (x) => AcosImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Asin, (x) => AsinImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Atan, (x) => AtanImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Acosh, (x) => AcoshImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Asinh, (x) => AsinhImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Atanh, (x) => AtanhImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Atan2, (x, y) => Atan2Implementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Radians, (x) => RadiansImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Degrees, (x) => DegreesImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Abs, (x) => AbsImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsArithmetic.Uri, FunctionsArithmetic.Sign, (x) => SignImplementation(x));

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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)x.AsDouble + y.AsDecimal));
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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong + y.AsDecimal));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal + y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal + (decimal)y.AsDouble));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal + y.AsDecimal));
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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)x.AsDouble - y.AsDecimal));
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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong - y.AsDecimal));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal - y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal - (decimal)y.AsDouble));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal - y.AsDecimal));
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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)x.AsDouble * y.AsDecimal));
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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong * y.AsDecimal));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal * y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal * (decimal)y.AsDouble));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal * y.AsDecimal));
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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)x.AsDouble / y.AsDecimal));
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
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong / y.AsDecimal));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal / y.AsLong));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal / (decimal)y.AsDouble));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal / y.AsDecimal));
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
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(-x.AsDecimal));
            }
            return NullValue;
        }

        private static FlxValue ModuloImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble % y.AsDouble));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDouble % (double)y.AsLong));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)x.AsDouble % y.AsDecimal));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    try
                    {
                        return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong % y.AsLong));
                    }
                    catch (DivideByZeroException)
                    {
                        return FlxValue.FromBytes(FlexBuffer.SingleValue(double.NaN));
                    }

                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong % y.AsDouble));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsLong % y.AsDecimal));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal % y.AsLong));
                }
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal % (decimal)y.AsDouble));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(x.AsDecimal % y.AsDecimal));
                }
            }
            return NullValue;
        }

        private static FlxValue PowerImplementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Pow(x.AsDouble, y.AsDouble)));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Pow(x.AsDouble, y.AsLong)));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    // Will loose some precision here
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)Math.Pow(x.AsDouble, (double)y.AsDecimal)));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((long)Math.Pow(x.AsLong, y.AsLong)));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Pow(x.AsLong, y.AsDouble)));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    // Will loose some precision here
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)Math.Pow(x.AsLong, (double)y.AsDecimal)));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    // Will loose some precision here
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)Math.Pow((double)x.AsDecimal, y.AsLong)));
                }
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    // Will loose some precision here
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)Math.Pow((double)x.AsDecimal, y.AsDouble)));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    // Will loose some precision here
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)Math.Pow((double)x.AsDecimal, (double)y.AsDecimal)));
                }
            }
            return NullValue;
        }

        private static FlxValue SqrtImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sqrt(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sqrt(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)Math.Sqrt((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue ExpImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Exp(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Exp(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Exp((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue CosImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Cos(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Cos(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Cos((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue SinImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sin(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sin(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sin((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue TanImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Tan(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Tan(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Tan((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue CoshImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Cosh(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Cosh(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Cosh((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue SinhImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sinh(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sinh(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sinh((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue TanhImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Tanh(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Tanh(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                // Will loose some precision here
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Tanh((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue AcosImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Acos(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Acos(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Acos((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue AsinImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Asin(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Asin(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Asin((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue AtanImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue AcoshImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Acosh(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Acosh(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Acosh((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue AsinhImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Asinh(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Asinh(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Asinh((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue AtanhImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atanh(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atanh(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atanh((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue Atan2Implementation(FlxValue x, FlxValue y)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                // Float add float always return a float
                if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2(x.AsDouble, y.AsDouble)));
                }
                // Float add int, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2(x.AsDouble, y.AsLong)));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2(x.AsDouble, (double)y.AsDecimal)));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                // Int add int, will always return an int
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2(x.AsLong, y.AsLong)));
                }
                // Int add float, will always return a float
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2(x.AsLong, y.AsDouble)));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2(x.AsLong, (double)y.AsDecimal)));
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                if (y.ValueType == FlexBuffers.Type.Int)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2((double)x.AsDecimal, y.AsLong)));
                }
                else if (y.ValueType == FlexBuffers.Type.Float)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2((double)x.AsDecimal, y.AsDouble)));
                }
                else if (y.ValueType == FlexBuffers.Type.Decimal)
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Atan2((double)x.AsDecimal, (double)y.AsDecimal)));
                }
            }
            return NullValue;
        }

        private static double ConvertToRadians(double angle)
        {
            return (Math.PI / 180) * angle;
        }

        private static FlxValue RadiansImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(ConvertToRadians(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(ConvertToRadians(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(ConvertToRadians((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static double ConvertRadiansToDegrees(double radians)
        {
            double degrees = (180 / Math.PI) * radians;
            return (degrees);
        }

        private static FlxValue DegreesImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(ConvertRadiansToDegrees(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(ConvertRadiansToDegrees(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(ConvertRadiansToDegrees((double)x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue AbsImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Abs(x.AsDouble)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Abs(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Abs(x.AsDecimal)));
            }
            return NullValue;
        }

        private static FlxValue SignImplementation(FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                var v = x.AsDouble;
                // check if it is NaN then we return NaN according to substrait docs.
                if (double.IsNaN(v))
                {
                    return NaNValue;
                }
                return FlxValue.FromBytes(FlexBuffer.SingleValue((double)Math.Sign(v)));
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(Math.Sign(x.AsLong)));
            }
            else if (x.ValueType == FlexBuffers.Type.Decimal)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)Math.Sign(x.AsDecimal)));
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
