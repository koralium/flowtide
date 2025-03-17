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
using FlowtideDotNet.Connector.OpenFGA.Extensions;
using FlowtideDotNet.Core;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Model;

namespace FlowtideDotNet.Connector.OpenFGA.Tests
{
    internal class OpenFgaTestStream : FlowtideTestStream
    {
        private readonly ClientConfiguration clientConfiguration;
        private readonly bool addReadResolver;
        private readonly bool addWriteResolver;
        private readonly Func<OpenFgaClient, IAsyncEnumerable<TupleKey>>? deleteFilter;

        public OpenFgaTestStream(
            string testName, 
            ClientConfiguration clientConfiguration, 
            bool addReadResolver,
            bool addWriteResolver,
            Func<OpenFgaClient, IAsyncEnumerable<TupleKey>>? deleteFilter = null) : base(testName)
        {
            this.clientConfiguration = clientConfiguration;
            this.addReadResolver = addReadResolver;
            this.addWriteResolver = addWriteResolver;
            this.deleteFilter = deleteFilter;
        }

        protected override void AddWriteResolvers(IConnectorManager factory)
        {
            if (addWriteResolver) 
            {
                factory.AddOpenFGASink("openfga", new OpenFgaSinkOptions
                {
                    ClientConfiguration = clientConfiguration,
                    DeleteExistingDataFetcher = deleteFilter
                });
            }
            else
            {
                base.AddWriteResolvers(factory);
            }
        }

        protected override void AddReadResolvers(IConnectorManager factory)
        {
            if (addReadResolver)
            {
                factory.AddOpenFGASource("openfga", new OpenFgaSourceOptions
                {
                    ClientConfiguration = clientConfiguration
                });
            }
            else
            {
                base.AddReadResolvers(factory);
            }
        }
    }
}
