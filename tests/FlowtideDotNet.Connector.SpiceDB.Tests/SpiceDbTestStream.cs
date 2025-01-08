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

using Authzed.Api.V1;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.SpiceDB.Extensions;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using Grpc.Core;
using Grpc.Net.Client;

namespace FlowtideDotNet.Connector.SpiceDB.Tests
{
    internal class SpiceDbTestStream : FlowtideTestStream
    {
        private readonly GrpcChannel grpcChannel;
        private readonly bool addWriteResolver;
        private readonly bool addReadResolver;
        private readonly ReadRelationshipsRequest? deleteExistingFilter;

        public SpiceDbTestStream(
            string testName, 
            GrpcChannel grpcChannel,
            bool addWriteResolver,
            bool addReadResolver,
            ReadRelationshipsRequest? deleteExistingFilter = null) : base(testName)
        {
            this.grpcChannel = grpcChannel;
            this.addWriteResolver = addWriteResolver;
            this.addReadResolver = addReadResolver;
            this.deleteExistingFilter = deleteExistingFilter;
        }

        protected override void AddWriteResolvers(IConnectorManager factory)
        {
            if (addWriteResolver)
            {
                factory.AddSpiceDbSink("*", new SpiceDbSinkOptions()
                {
                    Channel = grpcChannel,
                    DeleteExistingDataFilter = deleteExistingFilter,
                    GetMetadata = () =>
                    {
                        var metadata = new Metadata();
                        metadata.Add("Authorization", $"Bearer {TestName}");
                        return metadata;
                    }
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
                factory.AddSpiceDbSource("*", new SpiceDbSourceOptions()
                {
                    Channel = grpcChannel,
                    Consistency = new Consistency()
                    {
                        FullyConsistent = true
                    },
                    GetMetadata = () =>
                    {
                        var metadata = new Metadata();
                        metadata.Add("Authorization", $"Bearer {TestName}");
                        return metadata;
                    }
                });
            }
            else
            {
                base.AddReadResolvers(factory);
            }
        }
    }
}
