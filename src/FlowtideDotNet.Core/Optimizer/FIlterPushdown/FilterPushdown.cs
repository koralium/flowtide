﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Optimizer.FIlterPushdown
{
    internal static class FilterPushdown
    {
        private static readonly object _emptyObject = new object();

        public static Plan Optimize(Plan plan)
        {
            Dictionary<int, List<ReferenceRelation>> referenceRelations = new Dictionary<int, List<ReferenceRelation>>();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                var relation = plan.Relations[i];
                var visitor = new JoinFilterPushdownVisitor();
                relation = visitor.Visit(relation, _emptyObject);

                plan.Relations[i] = relation;
            }

            return plan;
        }
    }
}
