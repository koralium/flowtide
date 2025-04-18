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
using Stowage;

namespace FlowtideDotNet.TpcDI.Extensions
{
    public static class IndustryExtensions
    {
        public static IConnectorManager AddIndustryData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            return connectorManager.AddCsvFileSource("industry_raw", new CsvFileOptions()
            {
                CsvColumns = new List<string>()
                {
                    "IN_ID",
                    "IN_NAME",
                    "IN_SC_ID"
                },
                FileStorage = filesLocation,
                GetInitialFiles = () => Task.FromResult<IEnumerable<string>>(new List<string>()
                {
                    "Batch1/Industry.txt"
                }),
                Delimiter = "|"
            });
        }
    }
}
