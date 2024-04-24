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
using FlowtideDotNet.Connector.SqlServer;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.SqlServer.Tests.e2e
{
    internal class SqlServerTestStream : FlowtideTestStream
    {
        private readonly string connectionString;
        private readonly List<string>? customPrimaryKeys;

        public SqlServerTestStream(string testName, string connectionString, List<string>? customPrimaryKeys = null) : base(testName)
        {
            this.connectionString = connectionString;
            this.customPrimaryKeys = customPrimaryKeys;
        }

        protected override void AddReadResolvers(ReadWriteFactory factory)
        {
            factory.AddSqlServerSource("*", () => connectionString);
        }

        protected override void AddWriteResolvers(ReadWriteFactory factory)
        {
            factory.AddSqlServerSink("*", new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => connectionString,
                CustomPrimaryKeys = customPrimaryKeys
            });
        }
    }
}
