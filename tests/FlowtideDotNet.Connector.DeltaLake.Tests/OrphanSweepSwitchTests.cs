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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(MaintenanceSwitchCollection))]
    public partial class OrphanSweepSwitchTests : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "osw-" + ScratchOwners.ShortId());

        public OrphanSweepSwitchTests()
        {
            Directory.CreateDirectory(_directory);
            Switch(false);
        }

        public void Dispose()
        {
            Switch(false);
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        private static void Switch(bool on) => AppContext.SetSwitch(MaintenanceSwitches.DisableMaintenanceDeletes, on);
    }
}
