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

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using System;
using System.Collections.Generic;
using System.Text;

namespace FlowtideDotNet.Storage.S3.Tests
{
    public class S3ContainerFixture : IAsyncLifetime
    {
        private IContainer? container;
        public S3ContainerFixture()
        {
            
        }

        public async Task DisposeAsync()
        {
            if (container != null)
            {
                await container.DisposeAsync();
            }
        }

        private sealed class WaitUntil : IWaitUntil
        {
            private static readonly IEnumerable<string> Pattern = ["Started S3MockApplication"];

            public async Task<bool> UntilAsync(IContainer container)
            {
                string item = (await container.GetLogsAsync(default(DateTime), default(DateTime), timestampsEnabled: false).ConfigureAwait(continueOnCapturedContext: false)).Item1;
                return Pattern.Any(new Func<string, bool>(item.Contains));
            }
        }

        public async Task InitializeAsync()
        {
            container = new ContainerBuilder("adobe/s3mock")
                .WithPortBinding(9090, true)
                .WithWaitStrategy(Wait.ForUnixContainer().AddCustomWaitStrategy(new WaitUntil()))
                .Build();
            await container.StartAsync();
        }

        public FlowtideS3Options GetOptions(string bucket)
        {
            if (container == null)
            {
                throw new InvalidOperationException("Container is not started");
            }
            return new FlowtideS3Options()
            {
                BucketName = bucket,
                Endpoint = $"http://localhost:{container.GetMappedPublicPort(9090)}",
                AccessKey = "dummy-access-key",
                SecretKey = "dummy-secret-key",
                ForcePathStyle = true
            };
        }
    }
}
