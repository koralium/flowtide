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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal class ParameterReplacerVisitor : System.Linq.Expressions.ExpressionVisitor
    {
        private readonly ParameterExpression toReplace;
        private readonly Expression replacement;

        public ParameterReplacerVisitor(ParameterExpression toReplace, Expression replacement)
        {
            this.toReplace = toReplace;
            this.replacement = replacement;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == toReplace)
            {
                return replacement;
            }
            return base.VisitParameter(node);
        }
    }
}
