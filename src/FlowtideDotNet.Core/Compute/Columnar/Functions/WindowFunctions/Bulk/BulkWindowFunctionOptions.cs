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
    /// Option keys on <see cref="FlowtideDotNet.Substrait.Expressions.WindowFunction.Options"/> that the
    /// bulk window functions understand.
    /// </summary>
    internal static class BulkWindowFunctionOptions
    {
        /// <summary>
        /// Set by the optimizer when a filter directly above the window relation guarantees that rows with
        /// a row_number above this value are dropped. The row_number function then outputs null for those
        /// rows instead of maintaining exact numbers, so shifts only recompute up to the bound.
        /// </summary>
        public const string MaxRowNumber = "max_row_number";
    }
}
