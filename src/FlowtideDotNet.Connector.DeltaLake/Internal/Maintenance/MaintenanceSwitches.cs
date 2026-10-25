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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Process-wide switches an operator sets outside the sink's options.
    /// </summary>
    internal static class MaintenanceSwitches
    {
        /// <summary>
        /// The AppContext switch that stops every background maintenance delete; a sink run that starts with it on deletes its discards inline.
        /// </summary>
        public const string DisableMaintenanceDeletes = "FlowtideDotNet.DeltaLake.DisableMaintenanceDeletes";

        /// <summary>
        /// Reads the switch on every call, never from a snapshot; a switch that cannot be read counts as on.
        /// </summary>
        public static bool DeletesDisabled()
        {
            try
            {
                return AppContext.TryGetSwitch(DisableMaintenanceDeletes, out var disabled) && disabled;
            }
            catch
            {
                return true;
            }
        }
    }
}
