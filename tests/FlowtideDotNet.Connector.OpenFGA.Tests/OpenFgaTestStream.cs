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
using FlowtideDotNet.Connector.OpenFGA.Extensions;
using FlowtideDotNet.Core.Engine;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using OpenFga.Sdk.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.OpenFGA.Tests
{
    internal class OpenFgaTestStream : FlowtideTestStream
    {
        private readonly ClientConfiguration clientConfiguration;
        private readonly Func<OpenFgaClient, IAsyncEnumerable<TupleKey>>? deleteFilter;

        public OpenFgaTestStream(string testName, ClientConfiguration clientConfiguration, Func<OpenFgaClient, IAsyncEnumerable<TupleKey>>? deleteFilter = null) : base(testName)
        {
            this.clientConfiguration = clientConfiguration;
            this.deleteFilter = deleteFilter;
        }

        protected override void AddWriteResolvers(ReadWriteFactory factory)
        {
            factory.AddOpenFGASink("openfga", new OpenFgaSinkOptions
            {
                ClientConfiguration = clientConfiguration,
                DeleteExistingDataFetcher = deleteFilter
            });
            base.AddWriteResolvers(factory);
        }

        protected override void AddReadResolvers(ReadWriteFactory factory)
        {
            factory.AddOpenFGASource("openfga", new OpenFgaSourceOptions
            {
                ClientConfiguration = clientConfiguration
            });
            base.AddReadResolvers(factory);
        }
    }
}
