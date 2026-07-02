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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class TpcdiTests : FlowtideAcceptanceBase
    {
        public TpcdiTests(ITestOutputHelper testOutputHelper, bool usePersistentStorage = false) : base(testOutputHelper, usePersistentStorage)
        {
        }

        record DailyMarketAggAndDate(double value, DateTime date);

        record DailyMarketResult(
            string DM_S_SYMB, 
            double ClosePrice,
            double DayHigh,
            double DayLow, 
            long volume,
            DateTime DM_DATE,
            DailyMarketAggAndDate YearLow,
            DailyMarketAggAndDate YearHigh,
            long batchID);

        [Fact]
        public async Task DailyMarketWindowFunctionTest()
        {
            // 7200 daily market entries, 720 for 10 securities
            GenerateTpcDi(10, 720);
            await StartStream(@"
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
		                'value', dm.DM_LOW, 
		                'date', dm.DM_DATE
	                ), DM_LOW) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearLow,
                MAX_BY(named_struct(
		                'value', dm.DM_HIGH, 
		                'date', dm.DM_DATE
	                ), DM_HIGH) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearHigh,
                BatchID
            FROM dailymarkets dm
            ");
            await WaitForUpdate();

            void Validate()
            {
                List<DailyMarketResult> expected = new List<DailyMarketResult>();

                var historyBySymbol = DailyMarkets
                    .GroupBy(dm => dm.DM_S_SYMB ?? "")
                    .ToDictionary(
                        g => g.Key,
                        g => g.OrderBy(dm => dm.DM_DATE).ToList()
                    );

                foreach (var group in historyBySymbol)
                {
                    var symbol = group.Key;
                    var history = group.Value;
                    for (int i = 0; i < history.Count; i++)
                    {
                        var dm = history[i];
                        var windowStart = Math.Max(0, i - 364);
                        var windowEnd = i;
                        var window = history.GetRange(windowStart, windowEnd - windowStart + 1);
                        var yearLow = window.Min(dm => dm.DM_LOW) ?? 0;
                        var yearLowDate = window.FirstOrDefault(dm => dm.DM_LOW == yearLow)?.DM_DATE ?? DateTime.MinValue;
                        var yearHigh = window.Max(dm => dm.DM_HIGH) ?? 0;
                        var yearHighDate = window.FirstOrDefault(dm => dm.DM_HIGH == yearHigh)?.DM_DATE ?? DateTime.MinValue;
                        expected.Add(new DailyMarketResult(
                            DM_S_SYMB: symbol,
                            ClosePrice: dm.DM_CLOSE ?? 0,
                            DayHigh: dm.DM_HIGH ?? 0,
                            DayLow: dm.DM_LOW ?? 0,
                            volume: dm.DM_VOL ?? 0,
                            DM_DATE: dm.DM_DATE ?? DateTime.MinValue,
                            YearLow: new DailyMarketAggAndDate(yearLow, yearLowDate),
                            YearHigh: new DailyMarketAggAndDate(yearHigh, yearHighDate),
                            batchID: dm.BatchID
                        ));
                    }
                }

                AssertCurrentDataEqual(expected);
            }

            Validate();

            GenerateDailyMarkets(1);
            
            await WaitForUpdate();
            
            Validate();
        }
    }
}
