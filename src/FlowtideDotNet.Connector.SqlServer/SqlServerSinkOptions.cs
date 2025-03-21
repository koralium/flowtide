﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core.Operators.Write;

namespace FlowtideDotNet.Connector.SqlServer
{
    public class SqlServerSinkOptions
    {
        public required Func<string> ConnectionStringFunc { get; set; }

        /// <summary>
        /// Set custom primary keys for the table.
        /// If not set, the primary keys are collected from the database.
        /// </summary>
        public List<string>? CustomPrimaryKeys { get; set; }

        /// <summary>
        /// If set to false, the sink will look at any database on the server that the connection string points to.
        /// </summary>
        public bool UseDatabaseDefinedInConnectionStringOnly { get; set; }

        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;
    }
}
