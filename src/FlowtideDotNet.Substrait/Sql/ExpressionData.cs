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
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Sql
{
    public class ExpressionData
    {
        public ExpressionData(Expression expr, string name, SubstraitBaseType type)
        {
            Expr = expr;
            Name = name;
            Type = type;
        }

        public Expression Expr { get; }
        public string Name { get; }
        public SubstraitBaseType Type { get; }
    }
}
