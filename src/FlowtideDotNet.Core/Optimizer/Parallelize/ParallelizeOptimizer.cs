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

using FlowtideDotNet.Core.Optimizer.JoinProjectionPushdown;
using FlowtideDotNet.Substrait;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Optimizer.MergeJoinParallelize
{
    internal static class ParallelizeOptimizer
    {
        private static readonly object _emptyObject = new object();

        public static Plan Optimize(Plan plan, int parallelCount)
        {
            var visitor = new ParallelizeVisitor(parallelCount);
            visitor.VisitPlan(plan);
            return plan;
        }
    }
}
