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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using Google.Protobuf.WellKnownTypes;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal static class MergeJoinExpressionCompiler
    {
        internal static System.Linq.Expressions.MethodCallExpression CompareRef(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(FlxValueRefComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        private static System.Linq.Expressions.Expression GetAccessFieldExpression(System.Linq.Expressions.ParameterExpression parameter, FieldReference fieldReference, int relativeIndex)
        {
            if (fieldReference is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment referenceSegment)
            {
                var method = typeof(JoinStreamEvent).GetMethod("GetColumnRef");

                if (method == null)
                {
                    throw new InvalidOperationException("Method GetRef could not be found");
                }

                return System.Linq.Expressions.Expression.Call(parameter, method, System.Linq.Expressions.Expression.Constant(referenceSegment.Field - relativeIndex));
            }
            throw new NotSupportedException("Only direct field references are supported in merge join keys");
        }

        private static bool EqualImplementation(in FlxValueRef x, in FlxValueRef y)
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                return false;
            }
            else if (FlxValueRefComparer.CompareTo(x, y) == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        internal static System.Linq.Expressions.MethodCallExpression EqualRef(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(MergeJoinExpressionCompiler).GetMethod("EqualImplementation", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        public class MergeCompileResult
        {
            public MergeCompileResult(Func<JoinStreamEvent, JoinStreamEvent, int> leftCompare, Func<JoinStreamEvent, JoinStreamEvent, int> rightCompare, Func<JoinStreamEvent, JoinStreamEvent, int> seekCompare, Func<JoinStreamEvent, JoinStreamEvent, bool> checkCondition)
            {
                LeftCompare = leftCompare;
                RightCompare = rightCompare;
                SeekCompare = seekCompare;
                CheckCondition = checkCondition;
            }

            public Func<JoinStreamEvent, JoinStreamEvent, int> LeftCompare { get; set; }
            public Func<JoinStreamEvent, JoinStreamEvent, int> RightCompare { get; set; }
            public Func<JoinStreamEvent, JoinStreamEvent, int> SeekCompare { get; set; }
            public Func<JoinStreamEvent, JoinStreamEvent, bool> CheckCondition { get; set; }
        }

        public static MergeCompileResult Compile(MergeJoinRelation mergeJoinRelation)
        {
            var paramLeft = System.Linq.Expressions.Expression.Parameter(typeof(JoinStreamEvent));
            var paramRight = System.Linq.Expressions.Expression.Parameter(typeof(JoinStreamEvent));

            List<System.Linq.Expressions.Expression> leftIndexExpressions = new List<System.Linq.Expressions.Expression>();
            List<System.Linq.Expressions.Expression> rightIndexExpressions = new List<System.Linq.Expressions.Expression>();

            List<System.Linq.Expressions.Expression> seekExpressions = new List<System.Linq.Expressions.Expression>();
            List<System.Linq.Expressions.Expression> fieldEqualExpressions = new List<System.Linq.Expressions.Expression>();

            for (int i = 0; i < mergeJoinRelation.LeftKeys.Count; i++)
            {
                var leftKey = mergeJoinRelation.LeftKeys[i];
                var rightKey = mergeJoinRelation.RightKeys[i];

                // Create field access for both left and right parameters
                var leftKeyAccessLeft = GetAccessFieldExpression(paramLeft, leftKey, 0);
                var leftKeyAccessRight = GetAccessFieldExpression(paramRight, leftKey, 0);
                // Compare the same field but with different inputs, used for insertion
                var comparisonLeftKey = CompareRef(leftKeyAccessLeft, leftKeyAccessRight);

                leftIndexExpressions.Add(comparisonLeftKey);

                var rightKeyAccessLeft = GetAccessFieldExpression(paramLeft, rightKey, mergeJoinRelation.Left.OutputLength);
                var rightKeyAccessRight = GetAccessFieldExpression(paramRight, rightKey, mergeJoinRelation.Left.OutputLength);
                var comparisonRightKey = CompareRef(rightKeyAccessLeft, rightKeyAccessRight);

                rightIndexExpressions.Add(comparisonRightKey);

                // Create the seek comparison that is used when seeking for a value
                var seekCompare = CompareRef(leftKeyAccessLeft, rightKeyAccessRight);
                seekExpressions.Add(seekCompare);

                // Create equal expression used in the condition when looping over values.
                var fieldEquals = EqualRef(leftKeyAccessLeft, rightKeyAccessRight);
                fieldEqualExpressions.Add(fieldEquals);
            }

            // Create each index compare function that returns if a value is lesser or greater than the other value
            // These functions are used during insertion
            var tmpVar = System.Linq.Expressions.Expression.Variable(typeof(int));
            var leftCompare = leftIndexExpressions.Last();
            for (int i = leftIndexExpressions.Count - 2; i >= 0; i--)
            {
                var res = leftIndexExpressions[i];
                var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                var condition = System.Linq.Expressions.Expression.Condition(conditionTest, leftCompare, tmpVar);
                var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                leftCompare = block;
            }

            var rightCompare = rightIndexExpressions.Last();
            for (int i = rightIndexExpressions.Count - 2; i >= 0; i--)
            {
                var res = rightIndexExpressions[i];
                var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                var condition = System.Linq.Expressions.Expression.Condition(conditionTest, rightCompare, tmpVar);
                var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                rightCompare = block;
            }

            var leftLambda = System.Linq.Expressions.Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(leftCompare, paramLeft, paramRight);
            var leftComp = leftLambda.Compile();
            var rightLambda = System.Linq.Expressions.Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(rightCompare, paramLeft, paramRight);
            var rightComp = rightLambda.Compile();

            // Create the seek compare function
            var seekComparison = seekExpressions.Last();
            for (int i = seekExpressions.Count - 2; i >= 0; i--)
            {
                var res = seekExpressions[i];
                var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                var condition = System.Linq.Expressions.Expression.Condition(conditionTest, seekComparison, tmpVar);
                var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                seekComparison = block;
            }

            var seekLambda = System.Linq.Expressions.Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(seekComparison, paramLeft, paramRight);
            var seekComp = seekLambda.Compile();

            System.Linq.Expressions.Expression? keyEqualsExpression;

            var firstEqual = fieldEqualExpressions.First();
            keyEqualsExpression = firstEqual;
            for (int i = 1; i < fieldEqualExpressions.Count; i++)
            {
                keyEqualsExpression = System.Linq.Expressions.Expression.AndAlso(keyEqualsExpression, fieldEqualExpressions[i]);
            }

            var equalsLambda = System.Linq.Expressions.Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, bool>>(keyEqualsExpression, paramLeft, paramRight);
            var equalsComp = equalsLambda.Compile();

            return new MergeCompileResult(leftComp, rightComp, seekComp, equalsComp);
        }
    }
}
