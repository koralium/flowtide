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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Type;
using System.Diagnostics;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    public class ColumnarExpressionVisitor : ExpressionVisitor<System.Linq.Expressions.Expression, ColumnParameterInfo>
    {
        private readonly IFunctionsRegister functionsRegister;

        public ColumnarExpressionVisitor(IFunctionsRegister functionsRegister)
        {
            this.functionsRegister = functionsRegister;
        }

        private static RowEvent ConvertToRowEvent(EventBatchData data, int index)
        {
            var rowEvent = RowEventToEventBatchData.RowReferenceToRowEvent(0, 0, new ColumnStore.TreeStorage.ColumnRowReference()
            {
                referenceBatch = data,
                RowIndex = index
            });
            return rowEvent;
        }

        private static IDataValue ConvertFlxValueToDataValue(FlxValue input)
        {
            return RowEventToEventBatchData.FlxValueToDataValue(input);
        }

        public override System.Linq.Expressions.Expression? VisitScalarFunction(ScalarFunction scalarFunction, ColumnParameterInfo state)
        {
            if (functionsRegister.TryGetColumnScalarFunction(scalarFunction.ExtensionUri, scalarFunction.ExtensionName, out var functionDef))
            {
                return functionDef.MapFunc(scalarFunction, state, this, functionsRegister.FunctionServices);
            }
            if (functionsRegister.TryGetScalarFunction(scalarFunction.ExtensionUri, scalarFunction.ExtensionName, out var function))
            {
                // Use old implementation for now where rows are created from the columns at a specific index

                var parameterList = new List<System.Linq.Expressions.Expression>();
                for (int i = 0; i < state.BatchParameters.Count; i++)
                {
                    var convertToRowEventMethod = typeof(ColumnarExpressionVisitor).GetMethod(nameof(ConvertToRowEvent), BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
                    Debug.Assert(convertToRowEventMethod != null);
                    var convertExpr = System.Linq.Expressions.Expression.Call(convertToRowEventMethod, state.BatchParameters[i], state.IndexParameters[i]);
                    parameterList.Add(convertExpr);
                }

                var visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(RowEvent));
                var func = function.MapFunc(scalarFunction, new ParametersInfo(parameterList, state.RelativeIndices), visitor);
                var convertToDataValueMethod = typeof(ColumnarExpressionVisitor).GetMethod(nameof(ConvertFlxValueToDataValue), BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
                Debug.Assert(convertToDataValueMethod != null);
                return System.Linq.Expressions.Expression.Call(convertToDataValueMethod, func);
            }
            return base.VisitScalarFunction(scalarFunction, state);
        }

        public override System.Linq.Expressions.Expression? VisitStringLiteral(StringLiteral stringLiteral, ColumnParameterInfo state)
        {
            return System.Linq.Expressions.Expression.Constant(new StringValue(stringLiteral.Value), typeof(IDataValue));
        }

        private static IDataValue GetColumnValue(in EventBatchData data, in int column, in int index, ReferenceSegment? segment, in DataValueContainer result)
        {
            data.GetColumn(column).GetValueAt(index, result, segment);
            return result;
        }

        /// <summary>
        /// Creates the following code:
        /// 
        /// batch.GetColumn(columnIndex).GetValueAt(indexParameter, resultDataValue, childReferenceSegment);
        /// </summary>
        /// <param name="directFieldReference"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        public override System.Linq.Expressions.Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, ColumnParameterInfo state)
        {
            // We must first check that it is a reference segment to find the relative index in case of a join
            if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                int parameterIndex = 0;
                int relativeIndex = 0;
                for (int i = 1; i < state.BatchParameters.Count; i++)
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

                var getValueMethod = typeof(ColumnarExpressionVisitor).GetMethod("GetColumnValue", BindingFlags.NonPublic | BindingFlags.Static);
                Debug.Assert(getValueMethod != null);

                var getValueExpr = System.Linq.Expressions.Expression.Call(
                    getValueMethod,
                    state.BatchParameters[parameterIndex],
                    System.Linq.Expressions.Expression.Constant(structReferenceSegment.Field - relativeIndex),
                    state.IndexParameters[parameterIndex],
                    System.Linq.Expressions.Expression.Constant(structReferenceSegment.Child, typeof(ReferenceSegment)),
                    System.Linq.Expressions.Expression.Constant(new DataValueContainer()));

                return getValueExpr;
            }
            return base.VisitDirectFieldReference(directFieldReference, state);
        }

        public override System.Linq.Expressions.Expression? VisitBoolLiteral(BoolLiteral boolLiteral, ColumnParameterInfo state)
        {
            return System.Linq.Expressions.Expression.Constant(new BoolValue(boolLiteral.Value), typeof(IDataValue));
        }

        public override System.Linq.Expressions.Expression? VisitArrayLiteral(ArrayLiteral arrayLiteral, ColumnParameterInfo state)
        {
            List<System.Linq.Expressions.Expression> expressions = new List<System.Linq.Expressions.Expression>();
            foreach (var expr in arrayLiteral.Expressions)
            {
                expressions.Add(expr.Accept(this, state));
            }
            var array = System.Linq.Expressions.Expression.NewArrayInit(typeof(IDataValue), expressions);
            return base.VisitArrayLiteral(arrayLiteral, state);
        }

        public override System.Linq.Expressions.Expression? VisitNumericLiteral(NumericLiteral numericLiteral, ColumnParameterInfo state)
        {
            if (numericLiteral.Value % 1 == 0)
            {
                return System.Linq.Expressions.Expression.Constant(new Int64Value((long)numericLiteral.Value), typeof(IDataValue));
            }
            return System.Linq.Expressions.Expression.Constant(new DoubleValue((double)numericLiteral.Value), typeof(IDataValue));
        }

        public override System.Linq.Expressions.Expression? VisitBinaryLiteral(BinaryLiteral binaryLiteral, ColumnParameterInfo state)
        {
            return System.Linq.Expressions.Expression.Constant(new BinaryValue(binaryLiteral.Value), typeof(IDataValue));
        }

        public override System.Linq.Expressions.Expression? VisitCastExpression(CastExpression castExpression, ColumnParameterInfo state)
        {
            var visitState = state.UpdateResultDataValue(System.Linq.Expressions.Expression.Constant(new DataValueContainer()));
            var expr = Visit(castExpression.Expression, visitState);

            if (expr == null)
            {
                throw new InvalidOperationException("The expression to cast is null.");
            }

            switch (castExpression.Type.Type)
            {
                case SubstraitType.String:
                    return ColumnCastImplementations.CallCastToString(expr, state.ResultDataValue);
                case SubstraitType.Int32:
                case SubstraitType.Int64:
                    return ColumnCastImplementations.CallCastToInt(expr, state.ResultDataValue);
                case SubstraitType.Decimal:
                    return ColumnCastImplementations.CallCastToDecimal(expr, state.ResultDataValue);
                case SubstraitType.Bool:
                    return ColumnCastImplementations.CallCastToBool(expr, state.ResultDataValue);
                case SubstraitType.Fp32:
                case SubstraitType.Fp64:
                    return ColumnCastImplementations.CallCastToDouble(expr, state.ResultDataValue);
                case SubstraitType.TimestampTz:
                    return ColumnCastImplementations.CallCastToTimestamp(expr, state.ResultDataValue);
            }

            throw new InvalidOperationException($"The type {castExpression.Type.Type} is not supported in cast.");
        }

        public override System.Linq.Expressions.Expression? VisitNullLiteral(NullLiteral nullLiteral, ColumnParameterInfo state)
        {
            return System.Linq.Expressions.Expression.Constant(new NullValue(), typeof(IDataValue));
        }

        public override System.Linq.Expressions.Expression? VisitIfThen(IfThenExpression ifThenExpression, ColumnParameterInfo state)
        {
            System.Linq.Expressions.Expression? elseStatement = default;
            if (ifThenExpression.Else != null)
            {
                elseStatement = Visit(ifThenExpression.Else, state);
                Debug.Assert(elseStatement != null);
            }
            else
            {
                elseStatement = System.Linq.Expressions.Expression.Constant(new NullValue(), typeof(IDataValue));
            }

            var expr = elseStatement;
            for (int i = ifThenExpression.Ifs.Count - 1; i >= 0; i--)
            {
                var ifClause = ifThenExpression.Ifs[i];
                var ifStatement = Visit(ifClause.If, state);
                var thenStatement = Visit(ifClause.Then, state);
                Debug.Assert(ifStatement != null);
                Debug.Assert(thenStatement != null);

                if (!ifStatement.Type.Equals(typeof(bool)))
                {
                    MethodInfo? genericToBoolMethod = typeof(DataValueBoolFunctions).GetMethod("ToBool", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                    Debug.Assert(genericToBoolMethod != null);
                    var toBoolMethod = genericToBoolMethod.MakeGenericMethod(ifStatement.Type);
                    ifStatement = System.Linq.Expressions.Expression.Call(toBoolMethod, ifStatement);
                }

                expr = System.Linq.Expressions.Expression.Condition(ifStatement, thenStatement, expr);
            }

            return expr;
        }

        public override System.Linq.Expressions.Expression? VisitListNestedExpression(ListNestedExpression listNestedExpression, ColumnParameterInfo state)
        {
            List<System.Linq.Expressions.Expression> arrInitExpressions = new List<System.Linq.Expressions.Expression>();
            foreach (var expr in listNestedExpression.Values)
            {
                arrInitExpressions.Add(expr.Accept(this, state.UpdateResultDataValue(System.Linq.Expressions.Expression.Constant(new DataValueContainer()))));
            }
            var newArrayExpr = System.Linq.Expressions.Expression.NewArrayInit(typeof(IDataValue), arrInitExpressions);

            var newListValueExpr = System.Linq.Expressions.Expression.New(typeof(ListValue).GetConstructor([typeof(IDataValue[])])!, newArrayExpr);

            return newListValueExpr;
        }

        public override System.Linq.Expressions.Expression? VisitMapNestedExpression(MapNestedExpression mapNestedExpression, ColumnParameterInfo state)
        {
            List<System.Linq.Expressions.Expression> arrInitExpressions = new List<System.Linq.Expressions.Expression>();

            foreach (var pair in mapNestedExpression.KeyValues)
            {
                var keyExpr = pair.Key.Accept(this, state.UpdateResultDataValue(System.Linq.Expressions.Expression.Constant(new DataValueContainer())));
                var valueExpr = pair.Value.Accept(this, state.UpdateResultDataValue(System.Linq.Expressions.Expression.Constant(new DataValueContainer())));
                var keyValueExpr = System.Linq.Expressions.Expression.New(typeof(KeyValuePair<IDataValue, IDataValue>).GetConstructor([typeof(IDataValue), typeof(IDataValue)])!, keyExpr, valueExpr);
                arrInitExpressions.Add(keyValueExpr);
            }

            var newArrayExpr = System.Linq.Expressions.Expression.NewArrayInit(typeof(KeyValuePair<IDataValue, IDataValue>), arrInitExpressions);
            var ctor = typeof(MapValue).GetConstructor([typeof(KeyValuePair<IDataValue, IDataValue>[])]);
            Debug.Assert(ctor != null);
            return System.Linq.Expressions.Expression.New(ctor, newArrayExpr);
        }

        public override System.Linq.Expressions.Expression? VisitSingularOrList(SingularOrListExpression singularOrList, ColumnParameterInfo state)
        {
            ScalarFunction scalarFunction = new ScalarFunction()
            {
                ExtensionUri = FunctionsBoolean.Uri,
                ExtensionName = FunctionsBoolean.Or,
                Arguments = new List<Substrait.Expressions.Expression>()
            };

            foreach (var opt in singularOrList.Options)
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
    }
}
