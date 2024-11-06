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

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;

namespace FlowtideDotNet.Core.Optimizer.GetTimestamp
{
    internal class GetTimestampReplacer : ExpressionVisitor<Expression, object>
    {
        private readonly int fieldIndex;

        public bool ContainsGetTimestamp { get; private set; }

        public GetTimestampReplacer(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }
        public override Expression? VisitScalarFunction(ScalarFunction scalarFunction, object state)
        {
            if (scalarFunction.ExtensionUri == FunctionsDatetime.Uri &&
                scalarFunction.ExtensionName == FunctionsDatetime.GetTimestamp)
            {
                ContainsGetTimestamp = true;
                return new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = fieldIndex
                    }
                };
            }

            for (int i = 0; i < scalarFunction.Arguments.Count; i++)
            {
                scalarFunction.Arguments[i] = Visit(scalarFunction.Arguments[i], state)!;
            }

            return scalarFunction;
        }

        public override Expression? VisitArrayLiteral(ArrayLiteral arrayLiteral, object state)
        {
            return arrayLiteral;
        }

        public override Expression? VisitBoolLiteral(BoolLiteral boolLiteral, object state)
        {
            return boolLiteral;
        }

        public override Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, object state)
        {
            return directFieldReference;
        }

        public override Expression? VisitIfThen(IfThenExpression ifThenExpression, object state)
        {
            if (ifThenExpression.Else != null)
            {
                ifThenExpression.Else = Visit(ifThenExpression.Else, state);
            }

            for (int i = 0; i < ifThenExpression.Ifs.Count; i++)
            {
                ifThenExpression.Ifs[i].If = Visit(ifThenExpression.Ifs[i].If, state)!;
                ifThenExpression.Ifs[i].Then = Visit(ifThenExpression.Ifs[i].Then, state)!;
            }

            return ifThenExpression;
        }

        public override Expression? VisitListNestedExpression(ListNestedExpression listNestedExpression, object state)
        {
            for (int i = 0; i < listNestedExpression.Values.Count; i++)
            {
                listNestedExpression.Values[i] = Visit(listNestedExpression.Values[i], state)!;
            }
            return listNestedExpression;
        }

        public override Expression? VisitMapNestedExpression(MapNestedExpression mapNestedExpression, object state)
        {
            for (int i = 0; i < mapNestedExpression.KeyValues.Count; i++) 
            {
                mapNestedExpression.KeyValues[i] = new KeyValuePair<Expression, Expression>(
                    Visit(mapNestedExpression.KeyValues[i].Key, state)!,
                    Visit(mapNestedExpression.KeyValues[i].Value, state)!
                    );
            }
            return mapNestedExpression;
        }

        public override Expression? VisitMultiOrList(MultiOrListExpression multiOrList, object state)
        {
            for (int i = 0; i < multiOrList.Value.Count; i++)
            {
                multiOrList.Value[i] = Visit(multiOrList.Value[i], state)!;
            }

            for (int i = 0; i < multiOrList.Options.Count; i++)
            {
                for (int k = 0; k < multiOrList.Options[i].Fields.Count; k++)
                {
                    multiOrList.Options[i].Fields[k] = Visit(multiOrList.Options[i].Fields[k], state)!;
                }
            }
            return multiOrList;
        }

        public override Expression? VisitNullLiteral(NullLiteral nullLiteral, object state)
        {
            return nullLiteral;
        }

        public override Expression? VisitNumericLiteral(NumericLiteral numericLiteral, object state)
        {
            return numericLiteral;
        }

        public override Expression? VisitSingularOrList(SingularOrListExpression singularOrList, object state)
        {
            for (int i = 0; i < singularOrList.Options.Count; i++)
            {
                singularOrList.Options[i] = Visit(singularOrList.Options[i], state)!;
            }
            singularOrList.Value = Visit(singularOrList.Value, state)!;
            return singularOrList;
        }

        public override Expression? VisitStringLiteral(StringLiteral stringLiteral, object state)
        {
            return stringLiteral;
        }

        public override Expression? VisitCastExpression(CastExpression castExpression, object state)
        {
            castExpression.Expression = Visit(castExpression.Expression, state)!;
            return castExpression;
        }
    }
}
