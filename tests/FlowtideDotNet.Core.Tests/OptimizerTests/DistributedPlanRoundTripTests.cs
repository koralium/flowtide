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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class DistributedPlanRoundTripTests
    {
        private const string Tables = @"
            CREATE TABLE a (id any, val any, amount decimal);
            CREATE TABLE b (id any, val any);
            ";

        // Optimized as the Orleans stream grain does before it sends the plan
        [Theory]
        [InlineData("SELECT a.val FROM a a INNER JOIN b b ON a.id = b.id")]
        [InlineData("SELECT a.val FROM a a LEFT JOIN b b ON a.id = b.id WHERE a.val < 10")]
        [InlineData("SELECT a.val FROM a a INNER JOIN b b ON a.id = b.id WHERE b.val = 'x' AND a.amount > 1.5")]
        [InlineData("SELECT count(*) FROM a GROUP BY val")]
        [InlineData("SELECT val, sum(amount), min(id) FROM a GROUP BY val")]
        [InlineData("SELECT count(*) FROM a")]
        [InlineData("SELECT DISTINCT val FROM a")]
        [InlineData("SELECT val FROM a UNION ALL SELECT val FROM b")]
        [InlineData("SELECT val FROM a UNION SELECT val FROM b")]
        [InlineData("SELECT val FROM a EXCEPT DISTINCT SELECT val FROM b")]
        [InlineData("SELECT val FROM a EXCEPT ALL SELECT val FROM b")]
        [InlineData("SELECT val FROM a INTERSECT DISTINCT SELECT val FROM b")]
        [InlineData("SELECT a.val, b.val FROM a a FULL OUTER JOIN b b ON a.id = b.id")]
        [InlineData("SELECT a.val FROM a a INNER JOIN b b ON a.id < b.id")]
        [InlineData("SELECT val, row_number() OVER (PARTITION BY id ORDER BY val) FROM a")]
        [InlineData("SELECT CASE WHEN val = 1 THEN 'one' END, coalesce(val, 0) FROM a")]
        [InlineData("SELECT a.val FROM a a WHERE EXISTS (SELECT 1 FROM b b WHERE b.id = a.id)")]
        public void AnOptimizedDistributedPlanSurvivesARoundTrip(string query)
        {
            foreach (int substreams in new int[] { 1, 2, 3 })
            {
                var builder = new SqlPlanBuilder();
                builder.Sql(Tables + "INSERT INTO output " + query + ";");
                var plan = PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings()
                {
                    Parallelization = 1,
                    SimplifyProjection = true,
                    DistributedPlanOptions = new DistributedPlanOptions() { SubstreamCount = substreams }
                });

                PlanRoundTrip.AssertSurvives(plan);
            }
        }
    }
}
