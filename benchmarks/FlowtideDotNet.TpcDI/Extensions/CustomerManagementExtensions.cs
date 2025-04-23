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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Substrait.Type;
using SqlParser.Ast;
using Stowage;

namespace FlowtideDotNet.TpcDI.Extensions
{
    public static class CustomerManagementExtensions
    {
        public static IConnectorManager AddCustomerManagementData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            connectorManager.AddXmlFileSource("customers_history", new XmlFileOptions()
            {
                ElementName = "Action",
                FileStorage = filesLocation,
                GetInitialFiles = (storage, state) => Task.FromResult<IEnumerable<string>>(new List<string>()
                {
                    "Batch1/CustomerMgmt.xml"
                }),
                XmlSchema = File.ReadAllText("customers.xsd")
            });

            connectorManager.AddCsvFileSource("customers_incremental_raw", new CsvFileOptions()
            {
                DeltaCsvColumns = new List<string>()
                {
                    "CDC_FLAG",
                    "CDC_DSN",
                    "C_ID",
                    "C_TAX_ID",
                    "C_ST_ID",
                    "C_L_NAME",
                    "C_F_NAME",
                    "C_M_NAME",
                    "C_GNDR",
                    "C_TIER",
                    "C_DOB",
                    "C_ADLINE1",
                    "C_ADLINE2",
                    "C_ZIPCODE",
                    "C_CITY",
                    "C_STATE_PROV",
                    "C_CTRY",
                    "C_CTRY_1",
                    "C_AREA_1",
                    "C_LOCAL_1",
                    "C_EXT_1",
                    "C_CTRY_2",
                    "C_AREA_2",
                    "C_LOCAL_2",
                    "C_EXT_2",
                    "C_CTRY_3",
                    "C_AREA_3",
                    "C_LOCAL_3",
                    "C_EXT_3",
                    "C_EMAIL_1",
                    "C_EMAIL_2",
                    "C_LCL_TX_ID",
                    "C_NAT_TX_ID"
                },
                CsvColumns = new List<string>(),
                FileStorage = filesLocation,
                GetInitialFiles = () => {
                    return Task.FromResult<IEnumerable<string>>(new List<string>());
                },
                DeltaGetNextFile = (lastFile, batchId) =>
                {
                    return $"Batch{batchId}/Customer.txt";
                },
                Delimiter = "|",
                BeforeBatch = async (batchId, state, storage) =>
                {
                    using var stream = await storage.OpenRead($"Batch{batchId}/BatchDate.txt");
                    if (stream == null)
                    {
                        throw new InvalidOperationException($"BatchDate.txt not found in Batch{batchId}");
                    }
                    // Read the line
                    using var reader = new StreamReader(stream);
                    var line = await reader.ReadLineAsync();
                    state["batchdate"] = line!;
                },
                ModifyRow = (original, output, batchId, fileName, state) =>
                {
                    output[output.Length - 2] = batchId.ToString();
                    output[output.Length - 1] = state["batchdate"];
                },
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                    {
                        "C_ID",
                        "C_TAX_ID",
                        "C_ST_ID",
                        "C_L_NAME",
                        "C_F_NAME",
                        "C_M_NAME",
                        "C_GNDR",
                        "C_TIER",
                        "C_DOB",
                        "C_ADLINE1",
                        "C_ADLINE2",
                        "C_ZIPCODE",
                        "C_CITY",
                        "C_STATE_PROV",
                        "C_CTRY",
                        "C_CTRY_1",
                        "C_AREA_1",
                        "C_LOCAL_1",
                        "C_EXT_1",
                        "C_CTRY_2",
                        "C_AREA_2",
                        "C_LOCAL_2",
                        "C_EXT_2",
                        "C_CTRY_3",
                        "C_AREA_3",
                        "C_LOCAL_3",
                        "C_EXT_3",
                        "C_EMAIL_1",
                        "C_EMAIL_2",
                        "C_LCL_TX_ID",
                        "C_NAT_TX_ID",
                        "BatchID",
                        "ActionTS"
                    },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new Int64Type(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new Int64Type(),
                            new TimestampType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new Int64Type(),
                            new TimestampType()
                        }
                    }
                }
            });

            connectorManager.AddCsvFileSource("account_incremental_raw", new CsvFileOptions()
            {
                CsvColumns = new List<string>(),
                FileStorage = filesLocation,
                GetInitialFiles = () =>
                {
                    return Task.FromResult<IEnumerable<string>>(new List<string>());
                },
                DeltaGetNextFile = (lastFile, batchId) =>
                {
                    return $"Batch{batchId}/Account.txt";
                },
                Delimiter = "|",
                BeforeBatch = async (batchId, state, storage) =>
                {
                    using var stream = await storage.OpenRead($"Batch{batchId}/BatchDate.txt");
                    if (stream == null)
                    {
                        throw new InvalidOperationException($"BatchDate.txt not found in Batch{batchId}");
                    }
                    // Read the line
                    using var reader = new StreamReader(stream);
                    var line = await reader.ReadLineAsync();
                    state["batchdate"] = line!;
                },
                ModifyRow = (original, output, batchId, fileName, state) =>
                {
                    output[output.Length - 2] = batchId.ToString();
                    output[output.Length - 1] = state["batchdate"];
                },
                DeltaCsvColumns = new List<string>()
                {
                    "CDC_FLAG",
                    "CDC_DSN",
                    "CA_ID",
                    "CA_B_ID",
                    "CA_C_ID",
                    "CA_NAME",
                    "CA_TAX_ST",
                    "CA_ST_ID"
                },
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                    {
                        "CA_ID",
                        "CA_B_ID",
                        "CA_C_ID",
                        "CA_NAME",
                        "CA_TAX_ST",
                        "CA_ST_ID",
                        "BatchID",
                        "ActionTS"
                    },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new Int64Type(),
                            new Int64Type(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new StringType(),
                            new Int64Type(),
                            new TimestampType()
                        }
                    }
                }
            });

            return connectorManager;
        }
    }
}
