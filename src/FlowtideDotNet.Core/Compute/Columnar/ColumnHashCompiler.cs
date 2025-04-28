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
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Flexbuffer;
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

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal static class ColumnHashCompiler
    {
        public static Func<EventBatchData, int, uint> CompileGetHashCode(List<Substrait.Expressions.Expression> expressions, FunctionsRegister functionsRegister)
        {
            // Each function is using its own xxHash32
            XxHash32 xxHash32 = new XxHash32();
            var hashConstant = System.Linq.Expressions.Expression.Constant(xxHash32);

            ColumnarExpressionVisitor visitor = new ColumnarExpressionVisitor(functionsRegister);
            var batchParameter = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData), "batch");
            var indexParameter = System.Linq.Expressions.Expression.Parameter(typeof(int), "index");

            var resultContainer = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
            var parametersInfo = new ColumnParameterInfo(new List<ParameterExpression>() { batchParameter }, new List<ParameterExpression>() { indexParameter }, new List<int>() { 0 }, resultContainer);

            List<System.Linq.Expressions.Expression> hashExpressions = new List<System.Linq.Expressions.Expression>();
            for (int i = 0; i < expressions.Count; i++)
            {
                var expr = expressions[i];

                if (expr is DirectFieldReference referenceExpression &&
                    referenceExpression.ReferenceSegment is StructReferenceSegment structReference)
                {
                    
                    // If it is a direct reference, use AddToHash on the column
                    // This improves performance by not having to fetch the value and then calculate the hash
                    // Mostly noticable on the complex data types.
                    hashExpressions.Add(CallAddToHashOnColumn(batchParameter, indexParameter, structReference.Child, structReference.Field, hashConstant));
                }
                else
                {
                    var compiledExpr = visitor.Visit(expr, parametersInfo);
                    Debug.Assert(compiledExpr != null);
                    if (compiledExpr.Type.IsAssignableTo(typeof(IDataValue)))
                    {
                        var addToHashMethod = typeof(IDataValue).GetMethod("AddToHash");
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
            var hashMethod = typeof(XxHash32).GetMethod("GetHashAndReset", new System.Type[] { typeof(Span<byte>) });
            Debug.Assert(hashMethod != null);
            var callGetHash = System.Linq.Expressions.Expression.Call(hashConstant, hashMethod, spanVariable);
            hashExpressions.Add(callGetHash);
            var readOnlySpanExpr = System.Linq.Expressions.Expression.Convert(spanVariable, typeof(ReadOnlySpan<byte>));
            var readInt32BigEndianMethod = typeof(BinaryPrimitives).GetMethod("ReadUInt32BigEndian");
            Debug.Assert(readInt32BigEndianMethod != null);
            var getFinalResultExpr = System.Linq.Expressions.Expression.Call(readInt32BigEndianMethod, readOnlySpanExpr);
            hashExpressions.Add(getFinalResultExpr);

            var blockExpr = System.Linq.Expressions.Expression.Block(typeof(uint), new List<ParameterExpression>() { spanVariable }, hashExpressions);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, uint>>(blockExpr, batchParameter, indexParameter);
            return lambda.Compile();
        }

        private static System.Linq.Expressions.Expression CallAddToHashOnColumn(
            ParameterExpression batchParameter, 
            ParameterExpression indexParameter, 
            ReferenceSegment? child, 
            int columnIndex, 
            ConstantExpression hashExpression)
        {
            var getColumnMethod = typeof(EventBatchData).GetMethod("GetColumn");
            if (getColumnMethod == null)
            {
                throw new InvalidOperationException("Could not find GetColumn method");
            }

            var columnExpr = System.Linq.Expressions.Expression.Call(batchParameter, getColumnMethod, System.Linq.Expressions.Expression.Constant(columnIndex));

            var addToHashMethod = typeof(IColumn).GetMethod("AddToHash");

            if (addToHashMethod == null)
            {
                throw new InvalidOperationException("Could not find AddToHash method");
            }

            var childExpression = System.Linq.Expressions.Expression.Constant(child, typeof(ReferenceSegment));
            var callAddToHash = System.Linq.Expressions.Expression.Call(columnExpr, addToHashMethod, indexParameter, childExpression, hashExpression);
            return callAddToHash;
        }
    }
}
