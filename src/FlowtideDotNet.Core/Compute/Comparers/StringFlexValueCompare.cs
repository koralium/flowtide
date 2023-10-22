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
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Comparers
{
    internal static class StringFlexValueCompare
    {
        private static MethodInfo methodInfo = GetMethod("Equal");

        private static MethodInfo GetMethod(string name)
        {
            var method = typeof(StringFlexValueCompare).GetMethod(name, BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            if (method == null)
            {
                throw new InvalidOperationException("Method could not be found");
            }
            return method;
        }

        public static Expression EqualsExpression(Expression flxValueExpr, Expression str) 
        {
            return Expression.Call(methodInfo, flxValueExpr, str);
        }

        public static bool Equal(FlxValue flxValue, string literal)
        {
            if (flxValue.ValueType != FlexBuffers.Type.String)
            {
                return false;
            }
            return flxValue.AsString.Equals(literal);
        }
    }
}
