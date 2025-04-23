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
    public static class TradeExtensions
    {
        public static IConnectorManager AddTradeData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            connectorManager.AddCsvFileSource("trade_batch1_raw", new CsvFileOptions()
            {
                Delimiter = "|",
                CsvColumns = new List<string>()
                {
                    "T_ID",
                    "T_DTS",
                    "T_ST_ID",
                    "T_TT_ID",
                    "T_IS_CASH",
                    "T_S_SYMB",
                    "T_QTY",
                    "T_BID_PRICE",
                    "T_CA_ID",
                    "T_EXEC_NAME",
                    "T_TRADE_PRICE",
                    "T_CHRG",
                    "T_COMM",
                    "T_TAX"
                },
                FileStorage = filesLocation,
                GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()
                {
                    "Batch1/Trade.txt"
                }),
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                    {
                        "T_ID",
                        "T_DTS",
                        "T_ST_ID",
                        "T_TT_ID",
                        "T_IS_CASH",
                        "T_S_SYMB",
                        "T_QTY",
                        "T_BID_PRICE",
                        "T_CA_ID",
                        "T_EXEC_NAME",
                        "T_TRADE_PRICE",
                        "T_CHRG",
                        "T_COMM",
                        "T_TAX"
                },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new Int64Type(),
                            new TimestampType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new Int64Type(),
                            new DecimalType(),
                            new StringType(),
                            new StringType(),
                            new DecimalType(),
                            new DecimalType(),
                            new DecimalType(),
                            new DecimalType(),
                        }
                    }
                }
            });

            return connectorManager.AddCsvFileSource("trade_incremental_raw", new CsvFileOptions()
            {
                Delimiter = "|",
                CsvColumns = new List<string>(),
                DeltaCsvColumns = new List<string>()
                {
                    "CDC_FLAG",
                    "CDC_DSN",
                    "T_ID",
                    "T_DTS",
                    "T_ST_ID",
                    "T_TT_ID",
                    "T_IS_CASH",
                    "T_S_SYMB",
                    "T_QTY",
                    "T_BID_PRICE",
                    "T_CA_ID",
                    "T_EXEC_NAME",
                    "T_TRADE_PRICE",
                    "T_CHRG",
                    "T_COMM",
                    "T_TAX"
                },
                FileStorage = filesLocation,
                GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()),
                DeltaGetNextFile = (lastFile, batchId) =>
                {
                    return $"Batch{batchId}/Trade.txt";
                },
                ModifyRow = (original, output, batchId, fileName, state) =>
                {
                    output[output.Length - 1] = batchId.ToString();
                },
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                    {
                        "T_ID",
                        "T_DTS",
                        "T_ST_ID",
                        "T_TT_ID",
                        "T_IS_CASH",
                        "T_S_SYMB",
                        "T_QTY",
                        "T_BID_PRICE",
                        "T_CA_ID",
                        "T_EXEC_NAME",
                        "T_TRADE_PRICE",
                        "T_CHRG",
                        "T_COMM",
                        "T_TAX",
                        "BatchID"
                },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new Int64Type(),
                            new TimestampType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new Int64Type(),
                            new DecimalType(),
                            new StringType(),
                            new StringType(),
                            new DecimalType(),
                            new DecimalType(),
                            new DecimalType(),
                            new DecimalType(),
                            new Int64Type()
                        }
                    }
                }
            });
        }
    }
}
