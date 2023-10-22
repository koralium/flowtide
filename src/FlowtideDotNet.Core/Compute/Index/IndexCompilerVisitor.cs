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

using FlowtideDotNet.Core.Operators.Join;
using FlexBuffers;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Functions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;
using System.Linq.Expressions;
using static FlowtideDotNet.Core.Compute.Index.IndexCompilerVisitor;

namespace FlowtideDotNet.Core.Compute.Index
{
    /// <summary>
    /// Creates a comparer that is used during indexing to create a correct order of rows
    /// </summary>
    internal class IndexCompilerVisitor : ExpressionVisitor<System.Linq.Expressions.Expression, State>
    {
        public struct State
        {
            public ParameterExpression Parameter;
            public int RelativeIndex;
        }

        public override System.Linq.Expressions.Expression VisitAndFunction(AndFunction andFunction, State state)
        {
            throw new NotSupportedException("Index cannot have nested AND");
        }

        internal static System.Linq.Expressions.Expression AccessRootVector(ParameterExpression p)
        {
            var props = typeof(JoinStreamEvent).GetProperties().FirstOrDefault(x => x.Name == "Vector");
            var getMethod = props.GetMethod;
            return System.Linq.Expressions.Expression.Property(p, getMethod);
            //return System.Linq.Expressions.Expression.MakeMemberAccess(p, getMethod);
        }

        public override System.Linq.Expressions.Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, State state)
        {
            if (directFieldReference.ReferenceSegment is StructReferenceSegment referenceSegment)
            {
                var method = typeof(FlxVector).GetMethod("Get");
                return System.Linq.Expressions.Expression.Call(AccessRootVector(state.Parameter), method, System.Linq.Expressions.Expression.Constant(referenceSegment.Field - state.RelativeIndex));
                //return System.Linq.Expressions.Expression.ArrayAccess(AccessRootVector(state), System.Linq.Expressions.Expression.Constant(referenceSegment.Field));
            }
            throw new NotImplementedException();
        }

        public override System.Linq.Expressions.Expression? VisitEqualsFunction(EqualsFunction equalsFunction, State state)
        {
            throw new NotSupportedException("Index cannot have nested equals");
        }

        public override System.Linq.Expressions.Expression? VisitStringLiteral(StringLiteral stringLiteral, State state)
        {
            return System.Linq.Expressions.Expression.Constant(FlxValue.FromMemory(FlexBuffer.SingleValue(stringLiteral.Value)));
        }
    }
}
