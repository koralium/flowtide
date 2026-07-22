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

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Nexmark.Internal.Diagnosers;
using System.Threading.Tasks;

namespace FlowtideDotNet.Nexmark
{
    [EventCountDiagnoser]
    public class Query6 : QueryBase
    {
        [Benchmark]
        public async Task Q6()
        {
            await Stream.StartStream(@"
            INSERT INTO output
            SELECT
                Q.seller,
                AVG(Q.price) OVER
                    (PARTITION BY Q.seller ORDER BY Q.date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS avgprice
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id, seller ORDER BY price DESC) AS rownum
                FROM (SELECT A.id, A.seller, B.price, B.date_time
                    FROM auction AS A,
                        bid AS B
                    WHERE A.id = B.auction
                        and B.date_time between A.dateTime and A.expires) AS J
                WHERE rownum <= 1
            ) AS Q;
            ", planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings()
            {
                Parallelization = 1
            });
            await Stream.WaitForUpdate();
        }
    }
}
