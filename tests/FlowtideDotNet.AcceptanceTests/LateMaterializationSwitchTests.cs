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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core.Engine;

namespace FlowtideDotNet.AcceptanceTests
{
    public class LateMaterializationSwitchTests
    {
        [Theory]
        [InlineData(LateMaterializationMode.ForceOn, true)]
        [InlineData(LateMaterializationMode.Off, false)]
        public async Task TheModeDecidesWhetherAStreamGetsAStore(LateMaterializationMode mode, bool stores)
        {
            await using var stream = new FlowtideTestStream("LateMaterializationSwitch/" + mode) { LateMaterialization = mode };
            stream.Generate(100);
            await stream.StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
            await stream.WaitForUpdate();

            Assert.Equal(stores, stream.GetDiagnosticsGraph().Nodes.Values.Any(x => x.DisplayName == "Fetch required columns"));
            Assert.Equal(100, stream.GetActualRowsAsVectors().Count);
        }
    }
}
