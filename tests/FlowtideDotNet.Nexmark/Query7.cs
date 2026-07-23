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
    public class Query7 : QueryBase
    {
        [Benchmark]
        public async Task Q7()
        {
            await Stream.StartStream(@"
            INSERT INTO output
            SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
            FROM bid B
            JOIN (
              SELECT
                MAX(B1.price) AS maxprice,
                window_start,
                window_end
              FROM bid B1
              INNER JOIN tumbling_window(B1.date_time, 10, 'SECOND') wind
              GROUP BY window_start, window_end
            ) B1
            ON B.price = B1.maxprice AND
                B.date_time >= B1.window_start AND
                B.date_time < B1.window_end;
            ", planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings()
            {
                Parallelization = 4
            });
            await Stream.WaitForUpdate();
        }
    }
}
