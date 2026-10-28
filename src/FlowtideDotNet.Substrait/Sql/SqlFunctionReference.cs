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

namespace FlowtideDotNet.Substrait.Sql;

/// <summary>Reference documentation for the built-in Flowtide SQL functions.</summary>
/// <remarks>
/// <include file='../../../docs/reference/sql-functions.xml' path='documentation/topic[@id="functions"]/*' />
/// </remarks>
public static class SqlFunctionReference
{
    /// <summary>Supported scalar functions and SQL operators, including overloads and restrictions.</summary>
    /// <remarks>
    /// <include file='../../../docs/reference/sql-functions.xml' path='documentation/topic[@id="functions-scalar"]/*' />
    /// </remarks>
    public static class Scalar { }

    /// <summary>Supported aggregate functions, including empty-group, DISTINCT and FILTER behavior.</summary>
    /// <remarks>
    /// <include file='../../../docs/reference/sql-functions.xml' path='documentation/topic[@id="functions-aggregate"]/*' />
    /// </remarks>
    public static class Aggregate { }

    /// <summary>Supported OVER functions, ordering requirements and window frames.</summary>
    /// <remarks>
    /// <include file='../../../docs/reference/sql-functions.xml' path='documentation/topic[@id="functions-window"]/*' />
    /// </remarks>
    public static class Window { }

    /// <summary>Supported table functions, output columns and constant argument requirements.</summary>
    /// <remarks>
    /// <include file='../../../docs/reference/sql-functions.xml' path='documentation/topic[@id="functions-table"]/*' />
    /// </remarks>
    public static class Table { }
}
