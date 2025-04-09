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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.CheckFunctions
{
    internal class CheckFunctionFinderExpressionVisitor : ExpressionVisitor<bool, object>
    {
        public bool FoundCheckUsage { get; private set; } = false;
        public override bool VisitScalarFunction(ScalarFunction scalarFunction, object state)
        {
            if (scalarFunction.ExtensionUri == FunctionsCheck.Uri)
            {
                FoundCheckUsage = true;
            }
            return base.VisitScalarFunction(scalarFunction, state);
        }
    }

    internal class CheckFunctionFinderVisitor : BaseRelationExpressionVisitor<bool>
    {
        private CheckFunctionFinderExpressionVisitor _visitor;
        public CheckFunctionFinderVisitor()
        {
            _visitor = new CheckFunctionFinderExpressionVisitor();
        }
        public override ExpressionVisitor<bool, object> Visitor => _visitor;

        public bool ContainsCheckFunctions => _visitor.FoundCheckUsage;
    }

    internal static class CheckFunctionFinder
    {
        public static bool CheckPlan(Plan plan)
        {
            var visitor = new CheckFunctionFinderVisitor();

            foreach(var relation in plan.Relations)
            {
                visitor.Visit(relation, default!);
            }

            return visitor.ContainsCheckFunctions;
        }
    }

}
