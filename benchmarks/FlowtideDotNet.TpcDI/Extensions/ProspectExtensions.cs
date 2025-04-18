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
    public static class ProspectExtensions
    {
        public static IConnectorManager AddProspectData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            connectorManager.AddCsvFileSource("prospects_raw", new CsvFileOptions()
            {
                CsvColumns = new List<string>()
            {
                "AgencyID",
                "LastName",
                "FirstName",
                "MiddleInitial",
                "Gender",
                "AddressLine1",
                "AddressLine2",
                "PostalCode",
                "City",
                "State",
                "Country",
                "Phone",
                "Income",
                "NumberCars",
                "NumberChildren",
                "MaritalStatus",
                "Age",
                "CreditRating",
                "OwnOrRentFlag",
                "Employer",
                "NumberCreditCards",
                "NetWorth"
            },
                FileStorage = filesLocation,
                GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()
            {
                "Batch1/Prospect.csv"
            }),
                OutputSchema = new NamedStruct()
                {
                    Names = new List<string>()
                {
                    "AgencyID",
                    "LastName",
                    "FirstName",
                    "MiddleInitial",
                    "Gender",
                    "AddressLine1",
                    "AddressLine2",
                    "PostalCode",
                    "City",
                    "State",
                    "Country",
                    "Phone",
                    "Income",
                    "NumberCars",
                    "NumberChildren",
                    "MaritalStatus",
                    "Age",
                    "CreditRating",
                    "OwnOrRentFlag",
                    "Employer",
                    "NumberCreditCards",
                    "NetWorth",
                    "BatchDate",
                    "BatchID"
                },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                    {
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
                        new DecimalType(),
                        new Int64Type(),
                        new Int64Type(),
                        new StringType(),
                        new Int64Type(),
                        new Int64Type(),
                        new StringType(),
                        new StringType(),
                        new Int64Type(),
                        new DecimalType(),
                        new TimestampType(),
                        new Int64Type()
                    }
                    }
                },
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
                    output[original.Length] = state["batchdate"];
                    output[original.Length + 1] = batchId.ToString();
                }
            });

            return connectorManager;
        }
    }
}
