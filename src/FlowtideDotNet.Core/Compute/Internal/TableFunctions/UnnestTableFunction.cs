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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal.TableFunctions
{
    internal static class UnnestTableFunction
    {
        private static MethodInfo _unnestMethod = GetUnnestMethod();
        private static MethodInfo GetUnnestMethod()
        {
            var method = typeof(UnnestTableFunction).GetMethod(nameof(DoUnnest), BindingFlags.Static | BindingFlags.NonPublic);
            if (method == null)
            {
                throw new InvalidOperationException("Could not find unnest method");
            }
            return method;
        }

        public static void AddBuiltInUnnestFunction(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterTableFunction(FunctionsTableGeneric.Uri, FunctionsTableGeneric.Unnest, 
                (tableFunc, parameterInfo, visitor) =>
                {
                    if (tableFunc.Arguments.Count != 1)
                    {
                        throw new ArgumentException("Unnest function requires exactly one argument");
                    }
                    var expr = visitor.Visit(tableFunc.Arguments[0], parameterInfo);

                    if (expr == null)
                    {
                        throw new InvalidOperationException("Unnest function requires an argument");
                    }

                    return Expression.Call(_unnestMethod, expr);
                });
        }

        /// <summary>
        /// Function that does the actual unnesting of the array
        /// </summary>
        /// <param name="flxValue"></param>
        /// <returns></returns>
        private static IEnumerable<RowEvent> DoUnnest(FlxValue flxValue)
        {
            List<RowEvent> output = new List<RowEvent>();

            if (flxValue.ValueType == FlexBuffers.Type.Vector)
            {
                var vec = flxValue.AsVector;

                foreach (var item in vec)
                {
                    var row = new RowEvent(1, 0, new ArrayRowData(new FlxValue[] { item }));
                    output.Add(row);
                }
            }
            else if (flxValue.ValueType == FlexBuffers.Type.Map)
            {
                var map = flxValue.AsMap;

                foreach (var item in map)
                {
                    var mapBytes = FlexBufferBuilder.Map(m =>
                    {
                        m.Add("key", item.Key);
                        m.Add("value", item.Value);
                    });
                    var row = new RowEvent(1, 0, new ArrayRowData(new FlxValue[] { FlxValue.FromBytes(mapBytes) }));
                    output.Add(row);
                }
            }
            return output;
        }
    }
}
