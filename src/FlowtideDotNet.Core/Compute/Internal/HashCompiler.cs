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
using FlowtideDotNet.Core.Operators.Join;
using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Hashing;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class HashCompiler
    {
        /// <summary>
        /// Creates a function that computes the hash code for a stream event based on a list of expressions
        /// </summary>
        /// <param name="expressions"></param>
        /// <returns></returns>
        public static Func<StreamEvent, uint> CompileGetHashCode(List<Substrait.Expressions.Expression> expressions, FunctionsRegister functionsRegister)
        {
            // Each function is using its own xxHash32
            XxHash32 xxHash32 = new XxHash32();
            var hashConstant = System.Linq.Expressions.Expression.Constant(xxHash32);
            FlowtideExpressionVisitor visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(StreamEvent));
            var eventParameter = System.Linq.Expressions.Expression.Parameter(typeof(StreamEvent), "event");

            var parametersInfo = new ParametersInfo(new List<ParameterExpression>() { eventParameter }, new List<int>() { 0 });

            List<System.Linq.Expressions.Expression> hashExpressions = new List<System.Linq.Expressions.Expression>();
            for (int i = 0; i < expressions.Count; i++)
            {
                var expr = expressions[i];
                if (expr is DirectFieldReference referenceExpression &&
                    referenceExpression.ReferenceSegment is StructReferenceSegment structReference)
                {
                    // If it is a direct reference, use ref instead of the visitor
                    // This improves performance by not having to create multiple spans
                    hashExpressions.Add(CallAddToHashOnRef(eventParameter, referenceExpression, hashConstant));
                }
                else
                {
                    var compiledExpr = visitor.Visit(expr, parametersInfo);
                    Debug.Assert(compiledExpr != null);
                    if (compiledExpr.Type == typeof(FlxValueRef))
                    {
                        var addToHashMethod = typeof(FlxValueRef).GetMethod("AddToHash");
                        Debug.Assert(addToHashMethod != null);
                        hashExpressions.Add(System.Linq.Expressions.Expression.Call(compiledExpr, addToHashMethod, hashConstant));
                    }
                    else if (compiledExpr.Type == typeof(FlxValue))
                    {
                        var addToHashMethod = typeof(FlxValue).GetMethod("AddToHash");
                        Debug.Assert(addToHashMethod != null);
                        hashExpressions.Add(System.Linq.Expressions.Expression.Call(compiledExpr, addToHashMethod, hashConstant));
                    }
                    else
                    {
                        throw new InvalidOperationException("Got unknown return data type in hash compiler");
                    }
                }
            }

            var bytes = new byte[4];
            var destinationConstant = System.Linq.Expressions.Expression.Constant(new byte[4]);

            var spanVariable = System.Linq.Expressions.Expression.Variable(typeof(Span<byte>), "span");

            var destAsSpan = System.Linq.Expressions.Expression.New(typeof(Span<byte>).GetConstructor(new System.Type[] { typeof(byte[]) })!, destinationConstant);
            var assignToSpanExpr = System.Linq.Expressions.Expression.Assign(spanVariable, destAsSpan);

            hashExpressions.Add(assignToSpanExpr);

            // Call get hash and put it into the destination
            var hashMethod = typeof(XxHash32).GetMethod("GetHashAndReset", new System.Type[] {typeof(Span<byte>)});
            Debug.Assert(hashMethod != null);
            var callGetHash = System.Linq.Expressions.Expression.Call(hashConstant, hashMethod, spanVariable);
            hashExpressions.Add(callGetHash);
            var readOnlySpanExpr = System.Linq.Expressions.Expression.Convert(spanVariable, typeof(ReadOnlySpan<byte>));
            var readInt32BigEndianMethod = typeof(BinaryPrimitives).GetMethod("ReadUInt32BigEndian");
            Debug.Assert(readInt32BigEndianMethod != null);
            var getFinalResultExpr = System.Linq.Expressions.Expression.Call(readInt32BigEndianMethod, readOnlySpanExpr);
            hashExpressions.Add(getFinalResultExpr);

            var blockExpr = System.Linq.Expressions.Expression.Block(typeof(uint), new List<ParameterExpression>() { spanVariable }, hashExpressions);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<StreamEvent, uint>>(blockExpr, eventParameter);
            return lambda.Compile();
        }

        private static System.Linq.Expressions.Expression CallAddToHashOnRef(ParameterExpression eventParameter, DirectFieldReference referenceExpression, ConstantExpression xxHashConstant)
        {
            var accessRef = GetAccessFieldExpression(eventParameter, referenceExpression, 0);
            var addToHashMethod = typeof(FlxValueRef).GetMethod("AddToHash");
            Debug.Assert(addToHashMethod != null);
            return System.Linq.Expressions.Expression.Call(accessRef, addToHashMethod, xxHashConstant);
        }

        private static System.Linq.Expressions.Expression AccessRootVector(ParameterExpression p)
        {
            var props = typeof(StreamEvent).GetProperties().FirstOrDefault(x => x.Name == "Vector");
            var getMethod = props.GetMethod;
            return System.Linq.Expressions.Expression.Property(p, getMethod);
        }


        private static System.Linq.Expressions.Expression GetAccessFieldExpression(System.Linq.Expressions.ParameterExpression parameter, FieldReference fieldReference, int relativeIndex)
        {
            if (fieldReference is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment referenceSegment)
            {
                var method = typeof(FlxVector).GetMethod("GetRef");

                if (method == null)
                {
                    throw new InvalidOperationException("Method GetRef could not be found");
                }

                return System.Linq.Expressions.Expression.Call(AccessRootVector(parameter), method, System.Linq.Expressions.Expression.Constant(referenceSegment.Field - relativeIndex));
            }
            throw new NotSupportedException("Only direct field references are supported in merge join keys");
        }
    }
}
