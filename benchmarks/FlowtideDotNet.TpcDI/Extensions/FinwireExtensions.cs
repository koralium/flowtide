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
    public static class FinwireExtensions
    {
        public static IConnectorManager AddFinwireData(this IConnectorManager connectorManager, IFileStorage filesLocation)
        {
            return connectorManager.AddTextLinesFileSource("finwire_raw", new TextLinesFileOptions()
            {
                FileStorage = filesLocation,
                GetInitialFiles = async (location, state) =>
                {
                    var files = await location.Ls("Batch1/");
                    return files.Where(x => x.Name.StartsWith("FINWIRE") && !x.Name.EndsWith(".csv")).Select(x => x.Path.Full);
                }
            });
        }
    }
}
