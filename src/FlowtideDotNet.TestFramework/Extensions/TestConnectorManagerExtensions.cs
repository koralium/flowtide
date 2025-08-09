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

using FlowtideDotNet.Core;
using FlowtideDotNet.TestFramework;
using FlowtideDotNet.TestFramework.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core
{
    public static class TestConnectorManagerExtensions
    {
        public static IConnectorManager AddTestDataSink(
            this IConnectorManager connectorManager,
            string regexPattern,
            TestDataSink sink)
        {
            connectorManager.AddSink(new TestDataSinkFactory(regexPattern, sink));
            return connectorManager;
        }

        public static IConnectorManager AddTestDataTable(
            this IConnectorManager connectorManager,
            string tableName,
            TestDataTable table)
        {
            connectorManager.AddSource(new TestDataSourceFactory(tableName, table));
            return connectorManager;
        }
    }
}
