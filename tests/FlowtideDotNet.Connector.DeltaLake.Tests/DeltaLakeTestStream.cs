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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    internal class DeltaLakeTestStream : FlowtideTestStream
    {
        private readonly IFileStorage location;

        public DeltaLakeTestStream(string testName, IFileStorage location) : base(testName)
        {
            this.location = location;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddDeltaLakeSource(new DeltaLakeOptions() { StorageLocation = location, OneVersionPerCheckpoint = true, DeltaCheckInterval = TimeSpan.FromMilliseconds(100) });

            connectorManger.AddCatalog("catalogtest", c =>
            {
                c.AddDeltaLakeSource(new DeltaLakeOptions() { StorageLocation = location, OneVersionPerCheckpoint = true, DeltaCheckInterval = TimeSpan.FromMilliseconds(100) });
            });
        }
    }
}
