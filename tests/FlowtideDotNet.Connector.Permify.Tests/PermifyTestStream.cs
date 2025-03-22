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
using FlowtideDotNet.Core;
using Grpc.Net.Client;

namespace FlowtideDotNet.Connector.Permify.Tests
{
    internal class PermifyTestStream : FlowtideTestStream
    {
        private readonly string tenantId;
        private readonly GrpcChannel grpcChannel;
        private readonly bool addWriteResolver;
        private readonly bool addReadResolver;
        private int updateCounter = 0;

        public PermifyTestStream(
            string testName,
            string tenantId,
            GrpcChannel grpcChannel,
            bool addWriteResolver,
            bool addReadResolver) : base(testName)
        {
            this.tenantId = tenantId;
            this.grpcChannel = grpcChannel;
            this.addWriteResolver = addWriteResolver;
            this.addReadResolver = addReadResolver;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManger)
        {
            if (addReadResolver)
            {
                connectorManger.AddPermifyRelationshipSource("*", new PermifySourceOptions()
                {
                    Channel = grpcChannel,
                    TenantId = tenantId
                });
                return;
            }
            base.AddReadResolvers(connectorManger);
        }

        protected override void AddWriteResolvers(IConnectorManager connectorManger)
        {
            if (addWriteResolver)
            {
                connectorManger.AddPermifyRelationshipSink("*", new PermifySinkOptions()
                {
                    Channel = grpcChannel,
                    TenantId = tenantId,
                    OnWatermarkFunc = (watermark, token) =>
                    {
                        updateCounter++;
                        return Task.CompletedTask;
                    }
                });
                return;
            }
            base.AddWriteResolvers(connectorManger);
        }

        public override async Task WaitForUpdate()
        {
            if (addWriteResolver)
            {
                int currentCounter = updateCounter;
                while (updateCounter == currentCounter)
                {
                    await SchedulerTick();
                    await Task.Delay(10);
                }
            }
            else
            {
                await base.WaitForUpdate();
            }

        }
    }
}
