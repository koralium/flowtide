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

using FlowtideDotNet.Connector.Files;
using FlowtideDotNet.Core;
using FlowtideDotNet.Substrait.Type;
using Stowage;

namespace FlowtideDotNet.TpcDI.Extensions
{
    public static class DailyMarketExtensions
    {
        public static IConnectorManager AddDailyMarketData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            return connectorManager.AddCsvFileSource("dailymarket_raw", new CsvFileOptions()
            {
                CsvColumns = new List<string>()
                {
                    "DM_DATE",
                    "DM_S_SYMB",
                    "DM_CLOSE",
                    "DM_HIGH",
                    "DM_LOW",
                    "DM_VOL"
                },
                FileStorage = filesLocation,
                GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()
                {
                    "Batch1/DailyMarket.txt"
                }),
                Delimiter = "|",
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                    {
                        "DM_DATE",
                        "DM_S_SYMB",
                        "DM_CLOSE",
                        "DM_HIGH",
                        "DM_LOW",
                        "DM_VOL"
                    },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new TimestampType(),
                            new StringType(),
                            new Fp64Type(),
                            new Fp64Type(),
                            new Fp64Type(),
                            new Int64Type()
                        }
                    }
                }
            });
        }
    }
}
