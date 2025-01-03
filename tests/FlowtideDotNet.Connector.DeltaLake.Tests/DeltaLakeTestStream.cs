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

using DeltaLake.Interfaces;
using DeltaLake.Table;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    internal class DeltaLakeTestStream : FlowtideTestStream
    {
        private readonly List<string> primaryKeys;
        private readonly string tableLocation;
        private readonly ITable? table;

        public DeltaLakeTestStream(
            string testName,
            List<string> primaryKeys,
            string tableLocation,
            ITable? table = default) : base(testName)
        {
            this.primaryKeys = primaryKeys;
            this.tableLocation = tableLocation;
            this.table = table;
        }

        protected override void AddWriteResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddDeltaLakeSink(".*", new DeltaLakeSinkOptions()
            {
                PrimaryKeyColumns = primaryKeys,
                TableLocation = tableLocation,
                Table = table
            });
        }
    }
}
