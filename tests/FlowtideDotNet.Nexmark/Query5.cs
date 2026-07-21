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
    public class Query5 : QueryBase
    {
        [Benchmark]
        public async Task Q5()
        {
            await Stream.StartStream(@"
            INSERT INTO output
            SELECT AuctionBids.auction, AuctionBids.num
            FROM(
               SELECT
                 auction,
                 count(*) AS num,
                 window_start AS starttime,
                 window_end AS endtime
               FROM bid
               INNER JOIN hopping_window(date_time, 2, 'SECOND', 10, 'SECOND') wind
               GROUP BY
                 auction,
                 window_start,
                 window_end
            ) AS AuctionBids
            JOIN(
              SELECT
                max(CountBids.num) AS maxn,
                CountBids.starttime,
                CountBids.endtime
              FROM(
                SELECT
                count(*) AS num,
                window_start AS starttime,
                window_end AS endtime
                FROM bid
                INNER JOIN hopping_window(date_time, 2, 'SECOND', 10, 'SECOND') wind
                GROUP BY auction, window_start, window_end
              ) AS CountBids
            GROUP BY CountBids.starttime, CountBids.endtime
            ) AS MaxBids
            ON AuctionBids.starttime = MaxBids.starttime AND
                AuctionBids.endtime = MaxBids.endtime AND
                AuctionBids.num >= MaxBids.maxn;
            ", planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings()
            {
                Parallelization = 4
            });
            await Stream.WaitForUpdate();
        }
    }
}
