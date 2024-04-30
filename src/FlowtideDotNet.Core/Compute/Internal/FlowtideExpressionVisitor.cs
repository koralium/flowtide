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
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Buffers;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal class FlowtideExpressionVisitor : ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>
    {
        private readonly IFunctionsRegister functionsRegister;
        private readonly System.Type inputType;

        public FlowtideExpressionVisitor(IFunctionsRegister functionsRegister, System.Type inputType)
        {
            this.functionsRegister = functionsRegister;
            this.inputType = inputType;
        }

        public override System.Linq.Expressions.Expression? VisitScalarFunction(ScalarFunction scalarFunction, ParametersInfo state)
        {
            if (functionsRegister.TryGetScalarFunction(scalarFunction.ExtensionUri, scalarFunction.ExtensionName, out var def))
            {
                return def.MapFunc(scalarFunction, state, this);
            }
            else
            {
                throw new InvalidOperationException($"The scalar function {scalarFunction.ExtensionUri}:{scalarFunction.ExtensionName} is not implemented.");
            }
        }

        public static System.Linq.Expressions.Expression VisitInnerReferenceSegment(ReferenceSegment referenceSegment, System.Linq.Expressions.Expression expression)
        {
            // Since ref structs cant implement interfaces, check that methods exist in both flx value types
            Debug.Assert(nameof(FlxValue.GetVectorValue) != null);
            Debug.Assert(nameof(FlxValueRef.GetVectorValue) != null);
            Debug.Assert(nameof(FlxValue.GetMapValue) != null);
            Debug.Assert(nameof(FlxValueRef.GetMapValue) != null);

            var expr = referenceSegment switch
            {
                StructReferenceSegment { Field: >= 0 } structReferenceSegment => 
                    System.Linq.Expressions.Expression.Call(
                        expression, 
                        expression.Type.GetMethod(nameof(FlxValue.GetVectorValue)) ?? throw new MissingMethodException(nameof(FlxValue)), 
                        System.Linq.Expressions.Expression.Constant(structReferenceSegment.Field)),
                MapKeyReferenceSegment { Key: not null } mapKeyReferenceSegment => 
                    System.Linq.Expressions.Expression.Call(
                        expression, 
                        expression.Type.GetMethod(nameof(FlxValue.GetMapValue)) ?? throw new MissingMethodException(nameof(FlxValue)), 
                        System.Linq.Expressions.Expression.Constant(mapKeyReferenceSegment.Key)),
                _ => throw new NotImplementedException("The ${nameof(referenceSegment)} must be a {nameof(StructReferenceSegment)} with a positive {nameof(StructReferenceSegment.Field)}" or a ${nameof(MapKeyReferenceSegment)} with a non-null ${nameof(MapKeyReferenceSegment.Key)}"),
            };

            if (referenceSegment.Child != null)
            {
                return VisitInnerReferenceSegment(referenceSegment.Child, expr);
            }
            return expr;
        }

        public override System.Linq.Expressions.Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, ParametersInfo state)
        {
            // We must first check that it is a reference segment to find the relative index in case of a join
            if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                int parameterIndex = 0;
                int relativeIndex = 0;
                for (int i = 1; i < state.Parameters.Count; i++)
                {
                    if (structReferenceSegment.Field < state.RelativeIndices[i])
                    {
                        break;
                    }
                    else
                    {
                        relativeIndex = state.RelativeIndices[i];
                        parameterIndex = i;
                    }
                }
                var method = inputType.GetMethod(nameof(RowEvent.GetColumn));
                Debug.Assert(method != null);
                System.Linq.Expressions.Expression expr = System.Linq.Expressions.Expression.Call(state.Parameters[parameterIndex], method, System.Linq.Expressions.Expression.Constant(structReferenceSegment.Field - relativeIndex));
                if (structReferenceSegment.Child != null)
                {
                    expr = VisitInnerReferenceSegment(structReferenceSegment.Child, expr);
                }
                return expr;
            }
            return base.VisitDirectFieldReference(directFieldReference, state);
        }

        public override System.Linq.Expressions.Expression? VisitIfThen(IfThenExpression ifThenExpression, ParametersInfo state)
        {
            System.Linq.Expressions.Expression? elseStatement = default;
            if (ifThenExpression.Else != null)
            {
                elseStatement = Visit(ifThenExpression.Else, state);
            }
            else
            {
                elseStatement = System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.Null()));
            }

            var expr = elseStatement;
            for (int i = ifThenExpression.Ifs.Count - 1; i >= 0; i--)
            {
                var ifClause = ifThenExpression.Ifs[i];
                var ifStatement = Visit(ifClause.If, state);
                var thenStatement = Visit(ifClause.Then, state);

                if (ifStatement.Type.Equals(typeof(FlxValue)))
                {
                    MethodInfo toBoolMethod = typeof(FlxValueBoolFunctions).GetMethod("ToBool", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                    ifStatement = System.Linq.Expressions.Expression.Call(toBoolMethod, ifStatement);
                }

                expr = System.Linq.Expressions.Expression.Condition(ifStatement, thenStatement, expr);
            }

            return expr;
        }

        private static System.Linq.Expressions.MethodCallExpression ToArrayExpr(System.Linq.Expressions.Expression array)
        {
            MethodInfo toArrayMethod = typeof(FlxValueArrayFunctions).GetMethod("CreateArray", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(toArrayMethod, array);
        }

        public override System.Linq.Expressions.Expression? VisitArrayLiteral(ArrayLiteral arrayLiteral, ParametersInfo state)
        {
            List<System.Linq.Expressions.Expression> expressions = new List<System.Linq.Expressions.Expression>();
            foreach (var expr in arrayLiteral.Expressions)
            {
                expressions.Add(expr.Accept(this, state));
            }
            var array = System.Linq.Expressions.Expression.NewArrayInit(typeof(FlxValue), expressions);
            return ToArrayExpr(array);
        }

        public override System.Linq.Expressions.Expression? VisitBoolLiteral(BoolLiteral boolLiteral, ParametersInfo state)
        {
            return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue(boolLiteral.Value)));
        }

        public override System.Linq.Expressions.Expression? VisitNullLiteral(NullLiteral nullLiteral, ParametersInfo state)
        {
            return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.Null()));
        }

        public override System.Linq.Expressions.Expression? VisitNumericLiteral(NumericLiteral numericLiteral, ParametersInfo state)
        {
            // It is an integer number
            if (numericLiteral.Value % 1 == 0)
            {
                return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue((long)numericLiteral.Value)));
            }
            else
            {
                return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue((double)numericLiteral.Value)));
            }
        }

        public override System.Linq.Expressions.Expression? VisitStringLiteral(StringLiteral stringLiteral, ParametersInfo state)
        {
            return System.Linq.Expressions.Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue(stringLiteral.Value)));
        }

        public override System.Linq.Expressions.Expression? VisitSingularOrList(SingularOrListExpression singularOrList, ParametersInfo state)
        {
            // For now we convert the singular or list to a series of equals statements with OR between them.
            ScalarFunction scalarFunction = new ScalarFunction()
            {
                ExtensionUri = FunctionsBoolean.Uri,
                ExtensionName = FunctionsBoolean.Or,
                Arguments = new List<Substrait.Expressions.Expression>()
            };

            foreach(var opt in singularOrList.Options)
            {
                scalarFunction.Arguments.Add(new ScalarFunction()
                {
                    ExtensionUri = FunctionsComparison.Uri,
                    ExtensionName = FunctionsComparison.Equal,
                    Arguments = new List<Substrait.Expressions.Expression>()
                    {
                        singularOrList.Value,
                        opt
                    }
                });
            }

            return Visit(scalarFunction, state);
        }

        public override System.Linq.Expressions.Expression? VisitMapNestedExpression(MapNestedExpression mapNestedExpression, ParametersInfo state)
        {
            var builder = new FlexBuffer(ArrayPool<byte>.Shared);
            var builderConstant = System.Linq.Expressions.Expression.Constant(builder);
            List<System.Linq.Expressions.Expression> blockExpressions = new List<System.Linq.Expressions.Expression>();

            var newObjectMethod = typeof(FlexBuffer).GetMethod("NewObject", BindingFlags.Instance | BindingFlags.Public);
            var startVectorMethod = typeof(FlexBuffer).GetMethod("StartVector", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var addKeyMethod = typeof(FlexBuffer).GetMethod("AddKey", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var addValueMethod = typeof(FlexBuffer).GetMethod("Add", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, new System.Type[] { typeof(FlxValue) });
            var sortAndEndMapMethod = typeof(FlexBuffer).GetMethod("SortAndEndMap", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var finishMethod = typeof(FlexBuffer).GetMethod("Finish", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var flxValueFromBytesMethod = typeof(FlxValue).GetMethod("FromBytes", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic, new System.Type[] { typeof(byte[]) });
            var flxValueToStringMethod = typeof(FlxValueStringFunctions).GetMethod("ToString", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic, new System.Type[] { typeof(FlxValue) });
            Debug.Assert(newObjectMethod != null);
            Debug.Assert(startVectorMethod != null);
            Debug.Assert(addKeyMethod != null);

            var vectorStartVariable = System.Linq.Expressions.Expression.Variable(typeof(int), "vectorStart");
            var bytesVariable = System.Linq.Expressions.Expression.Variable(typeof(byte[]), "bytes");

            // Create a new object
            blockExpressions.Add(System.Linq.Expressions.Expression.Call(builderConstant, newObjectMethod));
            // Start the vector and assign vector start variable
            blockExpressions.Add(System.Linq.Expressions.Expression.Assign(vectorStartVariable, System.Linq.Expressions.Expression.Call(builderConstant, startVectorMethod)));
            
            // Add all the key value pairs to the map
            for (int i = 0; i < mapNestedExpression.KeyValues.Count; i++)
            {
                   var keyValue = mapNestedExpression.KeyValues[i];
                var key = keyValue.Key;
                var value = keyValue.Value;

                var keyExpr = Visit(key, state);

                var valueExpr = Visit(value, state);

                var addKeyCall = System.Linq.Expressions.Expression.Call(builderConstant, addKeyMethod, System.Linq.Expressions.Expression.Call(flxValueToStringMethod, keyExpr));
                blockExpressions.Add(addKeyCall);
                var addValueCall = System.Linq.Expressions.Expression.Call(builderConstant, addValueMethod, valueExpr);
                blockExpressions.Add(addValueCall);
            }

            // Sort and end map
            blockExpressions.Add(System.Linq.Expressions.Expression.Call(builderConstant, sortAndEndMapMethod, vectorStartVariable));
            // Finish
            blockExpressions.Add(System.Linq.Expressions.Expression.Assign(bytesVariable, System.Linq.Expressions.Expression.Call(builderConstant, finishMethod)));

            blockExpressions.Add(System.Linq.Expressions.Expression.Call(flxValueFromBytesMethod, bytesVariable));

            var blockExpr = System.Linq.Expressions.Expression.Block(typeof(FlxValue), new List<ParameterExpression>() { vectorStartVariable, bytesVariable }, blockExpressions);

            return blockExpr;
        }

        public override System.Linq.Expressions.Expression? VisitListNestedExpression(ListNestedExpression listNestedExpression, ParametersInfo state)
        {
            var builder = new FlexBuffer(ArrayPool<byte>.Shared);
            var builderConstant = System.Linq.Expressions.Expression.Constant(builder);
            List<System.Linq.Expressions.Expression> blockExpressions = new List<System.Linq.Expressions.Expression>();

            var newObjectMethod = typeof(FlexBuffer).GetMethod("NewObject", BindingFlags.Instance | BindingFlags.Public);
            var startVectorMethod = typeof(FlexBuffer).GetMethod("StartVector", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var addKeyMethod = typeof(FlexBuffer).GetMethod("AddKey", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var addValueMethod = typeof(FlexBuffer).GetMethod("Add", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, new System.Type[] { typeof(FlxValue) });
            var endVectorMethod = typeof(FlexBuffer).GetMethod("EndVector", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var finishMethod = typeof(FlexBuffer).GetMethod("Finish", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var flxValueFromBytesMethod = typeof(FlxValue).GetMethod("FromBytes", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic, new System.Type[] { typeof(byte[]) });
            var flxValueToStringMethod = typeof(FlxValueStringFunctions).GetMethod("ToString", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic, new System.Type[] { typeof(FlxValue) });
            Debug.Assert(newObjectMethod != null);
            Debug.Assert(startVectorMethod != null);
            Debug.Assert(addKeyMethod != null);

            var vectorStartVariable = System.Linq.Expressions.Expression.Variable(typeof(int), "vectorStart");
            var bytesVariable = System.Linq.Expressions.Expression.Variable(typeof(byte[]), "bytes");

            // Create a new object
            blockExpressions.Add(System.Linq.Expressions.Expression.Call(builderConstant, newObjectMethod));
            // Start the vector and assign vector start variable
            blockExpressions.Add(System.Linq.Expressions.Expression.Assign(vectorStartVariable, System.Linq.Expressions.Expression.Call(builderConstant, startVectorMethod)));

            // Add all the key value pairs to the map
            for (int i = 0; i < listNestedExpression.Values.Count; i++)
            {
                var value = listNestedExpression.Values[i];
                var valueExpr = Visit(value, state);
                var addValueCall = System.Linq.Expressions.Expression.Call(builderConstant, addValueMethod, valueExpr);
                blockExpressions.Add(addValueCall);
            }

            // End vector
            blockExpressions.Add(System.Linq.Expressions.Expression.Call(builderConstant, endVectorMethod, vectorStartVariable, System.Linq.Expressions.Expression.Constant(false), System.Linq.Expressions.Expression.Constant(false)));
            // Finish
            blockExpressions.Add(System.Linq.Expressions.Expression.Assign(bytesVariable, System.Linq.Expressions.Expression.Call(builderConstant, finishMethod)));

            blockExpressions.Add(System.Linq.Expressions.Expression.Call(flxValueFromBytesMethod, bytesVariable));

            var blockExpr = System.Linq.Expressions.Expression.Block(typeof(FlxValue), new List<ParameterExpression>() { vectorStartVariable, bytesVariable }, blockExpressions);

            return blockExpr;
        }

        public override System.Linq.Expressions.Expression? VisitCastExpression(CastExpression castExpression, ParametersInfo state)
        {
            var expr = Visit(castExpression.Expression, state);
            if (expr == null)
            {
                throw new InvalidOperationException("The expression to cast is null.");
            }
            switch (castExpression.Type.Type)
            {
                case Substrait.Type.SubstraitType.String:
                    return CastImplementations.CallCastToString(expr);
                case Substrait.Type.SubstraitType.Int64:
                case Substrait.Type.SubstraitType.Int32:
                    return CastImplementations.CallCastToInt(expr);
                case Substrait.Type.SubstraitType.Decimal:
                    return CastImplementations.CallCastToDecimal(expr);
                case Substrait.Type.SubstraitType.Bool:
                    return CastImplementations.CallCastToBool(expr);
                case Substrait.Type.SubstraitType.Fp32:
                case Substrait.Type.SubstraitType.Fp64:
                    return CastImplementations.CallCastToDouble(expr);
            }

            throw new InvalidOperationException($"The type {castExpression.Type.Type} is not supported in cast.");
        }
    }
}
