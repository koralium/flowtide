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
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;

namespace FlowtideDotNet.Benchmarks.Stream
{
    public class tpcDiBenchmarkConfiguration : ManualConfig
    {
        public tpcDiBenchmarkConfiguration()
        {
            AddJob(Job.Default.WithMaxIterationCount(10).WithMinIterationCount(1));
        }
    }

    [Config(typeof(tpcDiBenchmarkConfiguration))]

    public class TpcDiDailyMarketBenchmark
    {
        private int iterationId = 0;
        private BenchmarkTestStream? _stream;
        [IterationSetup]
        public void IterationSetup()
        {
            _stream = new BenchmarkTestStream(iterationId.ToString());
            _stream.GenerateTpcDi(1000, 720);
            _stream.CachePageCount = 100_000;
            iterationId++;
        }

        [Benchmark]
        public async Task DailyMarketBenchmark()
        {
            await _stream!.StartStream(@"
            INSERT INTO output 
            SELECT
                dm.DM_S_SYMB as DM_S_SYMB,
                DM_CLOSE as ClosePrice,
                DM_HIGH as DayHigh,
                DM_LOW as DayLow,
                DM_VOL as Volume,
                DM_DATE as DM_DATE,
                min_by(
	                named_struct(
		                'low', dm.DM_LOW, 
		                'date', dm.DM_DATE
	                ), DM_LOW) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearLow,
                MAX_BY(named_struct(
		                'high', dm.DM_HIGH, 
		                'date', dm.DM_DATE
	                ), DM_HIGH) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearHigh,
                BatchID
            FROM dailymarkets dm
            ", 1);
            await _stream.WaitForUpdate();
        }
    }
}
