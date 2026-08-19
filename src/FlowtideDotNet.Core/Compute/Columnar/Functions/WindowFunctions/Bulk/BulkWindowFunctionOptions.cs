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

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    /// <summary>
    /// Option keys the bulk window functions understand.
    /// </summary>
    internal static class BulkWindowFunctionOptions
    {
        /// <summary>
        /// Row numbers above this are output as null, set by the row_number filter hint.
        /// </summary>
        public const string MaxRowNumber = "max_row_number";

        /// <summary>
        /// Skips emitting rows the filter drops, only valid with a filter above.
        /// </summary>
        public const string EmitOnlyWithinMaxRowNumber = "emit_only_within_max_row_number";
    }
}
