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
    public class Query1 : QueryBase
    {
        [Benchmark]
        public async Task Q1()
        {
            await Stream.StartStream(@"
            INSERT INTO output
            SELECT
                auction,
                bidder,
                0.908 * price as price,
                date_time,
                extra
            FROM bid;
            ");
            await Stream.WaitForUpdate();
        }
    }
}
