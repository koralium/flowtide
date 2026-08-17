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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Nexmark
{
    [EventCountDiagnoser]
    public class Query8 : QueryBase
    {
        [Benchmark]
        public async Task Q8()
        {
            await Stream.StartStream(@"
            INSERT INTO output
            SELECT P.id, P.name, P.starttime
            FROM (
              SELECT P.id, P.name,
                     window_start AS starttime,
                     window_end AS endtime
              FROM person P
              INNER JOIN tumbling_window(P.datetime, 10, 'SECOND') wind
              GROUP BY P.id, P.name, window_start, window_end
            ) P
            JOIN (
              SELECT A.seller,
                     window_start AS starttime,
                     window_end AS endtime
              FROM auction A
              INNER JOIN tumbling_window(A.datetime, 10, 'SECOND') wind
              GROUP BY A.seller, window_start, window_end
            ) A
            ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;
            ", planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings()
            {
                Parallelization = 1
            });
            await Stream.WaitForUpdate();
        }
    }
}
