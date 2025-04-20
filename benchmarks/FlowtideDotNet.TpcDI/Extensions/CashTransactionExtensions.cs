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
    public static class CashTransactionExtensions
    {
        public static IConnectorManager AddCashTransactionData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            return connectorManager.AddCsvFileSource("cashtransaction_raw", new CsvFileOptions()
            {
                CsvColumns = new List<string>()
                {
                    "CT_CA_ID",
                    "CT_DTS",
                    "CT_AMT",
                    "CT_NAME"
                },
                FileStorage = filesLocation,
                GetInitialFiles = () =>
                {
                    return Task.FromResult<IEnumerable<string>>(["Batch1/CashTransaction.txt"]);
                },
                Delimiter = "|",
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                    {
                        "CT_CA_ID",
                        "CT_DTS",
                        "CT_AMT",
                        "CT_NAME"
                    },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new StringType(),
                            new TimestampType(),
                            new Fp64Type(),
                            new StringType()
                        }
                    }
                }
            });
        }
    }
}
