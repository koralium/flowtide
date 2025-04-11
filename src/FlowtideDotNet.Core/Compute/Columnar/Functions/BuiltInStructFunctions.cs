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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInStructFunctions
    {
        public static void AddBuiltInStructFunctions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnScalarFunction(FunctionsStruct.Uri, FunctionsStruct.Create,
                (func, parameterInfo, visitor, functionServices) =>
                {
                    if (func.Arguments.Count % 2 != 0)
                    {
                        throw new InvalidOperationException("Arguments must be in pairs of key and value.");
                    }
                    var argumentsLength = func.Arguments.Count / 2;

                    //var list = new List<IDataValue>();
                    //for (int i = 0; i < argumentsLength; i++)
                    //{
                    //    list.Add(NullValue.Instance);
                    //}
                    var arr = System.Linq.Expressions.Expression.Constant(new IDataValue[argumentsLength]);
                    string[] columnNames = new string[argumentsLength];
                    List<System.Linq.Expressions.Expression> expressions = new List<System.Linq.Expressions.Expression>();
                    for (int i = 0; i < func.Arguments.Count; i += 2)
                    {
                        var keyArg = func.Arguments[i];
                        var valueArg = func.Arguments[i + 1];

                        if (!(keyArg is StringLiteral keyStringLiteral))
                        {
                            throw new InvalidOperationException($"Argument {i} must be a string literal.");
                        }
                        var argIndex = i / 2;
                        columnNames[argIndex] = keyStringLiteral.Value;

                        var argResult = visitor.Visit(valueArg, parameterInfo);

                        if (argResult == null)
                        {
                            throw new InvalidOperationException($"Argument {i + 1} must be a valid expression.");
                        }

                        //var indexerProperty = typeof(List<IDataValue>).GetProperty("Item");
                        //if (indexerProperty == null)
                        //{
                        //    throw new InvalidOperationException("List<IDataValue> does not have an indexer property.");
                        //}
                        //var indexerMethod = indexerProperty.GetSetMethod();
                        //if (indexerMethod == null)
                        //{
                        //    throw new InvalidOperationException("List<IDataValue> indexer property does not have a setter.");
                        //}
                        //var argIndexConstant = System.Linq.Expressions.Expression.Constant(argIndex);


                        var arrAccess = System.Linq.Expressions.Expression.ArrayAccess(arr, System.Linq.Expressions.Expression.Constant(argIndex));
                        var assignArr = System.Linq.Expressions.Expression.Assign(arrAccess, argResult);
                        expressions.Add(assignArr);
                    }

                    var structValueCtor = typeof(StructValue).GetConstructor(new Type[] { typeof(StructHeader), typeof(IDataValue[]) }) ?? throw new InvalidOperationException("StructValue constructor not found.");
                    
                    StructHeader header = StructHeader.Create(columnNames);

                    var headerConstant = System.Linq.Expressions.Expression.Constant(header);

                    var newStructValue = System.Linq.Expressions.Expression.New(structValueCtor, headerConstant, arr);
                    expressions.Add(newStructValue);

                    var block = System.Linq.Expressions.Expression.Block(System.Linq.Expressions.Expression.Block(expressions));

                    return block;
                });
        }
    }
}
