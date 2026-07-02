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

using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Substrait.Sql
{
    public interface ITableProvider
    {
        bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata);

        /// <summary>
        /// Attempts to handle a table-valued function by name and arguments, producing a table function result if
        /// successful.
        /// </summary>
        /// <param name="functionName">The name of the table-valued function to handle. Cannot be null.</param>
        /// <param name="sqlTableFunction">The arguments to pass to the table-valued function. Cannot be null.</param>
        /// <param name="relation">When this method returns <see langword="true"/>, contains the resulting <see cref="TableProviderTableFunctionResult"/>; otherwise,
        /// <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if the table function was successfully handled and a table function result was produced;
        /// otherwise, <see langword="false"/>.</returns>
        bool TryHandleTableFunction(IReadOnlyList<string> functionName, TableProviderTableFunctionArguments sqlTableFunction, [NotNullWhen(true)] out TableProviderTableFunctionResult? relation);
    }
}
