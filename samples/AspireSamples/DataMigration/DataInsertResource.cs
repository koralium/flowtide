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

using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Azure;
using Aspire.Hosting.Lifecycle;
using Azure.ResourceManager;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AspireSamples.DataMigration
{
    internal class DataInsertLifecyclehook : IDistributedApplicationLifecycleHook, IAsyncDisposable
    {
        private readonly DistributedApplicationExecutionContext executionContext;
        private readonly ResourceNotificationService resourceNotificationService;
        private readonly ResourceLoggerService resourceLoggerService;
        private CancellationTokenSource tokenSource;
        public DataInsertLifecyclehook(DistributedApplicationExecutionContext executionContext,
            ResourceNotificationService resourceNotificationService,
            ResourceLoggerService resourceLoggerService)
        {
            tokenSource = new CancellationTokenSource();
            this.executionContext = executionContext;
            this.resourceNotificationService = resourceNotificationService;
            this.resourceLoggerService = resourceLoggerService;
        }

        public async Task BeforeStartAsync(DistributedApplicationModel appModel, CancellationToken cancellationToken = default)
        {
            
            var dataInsertResource = appModel.Resources.OfType<DataInsertResource>().SingleOrDefault();

            if (dataInsertResource is null)
            {
                return;
            }

            await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
            {
                ResourceType = "Data-Insert",
                State = "Starting"
            });
        }

        public Task AfterEndpointsAllocatedAsync(DistributedApplicationModel appModel, CancellationToken cancellationToken = default)
        {
            var dataInsertResource = appModel.Resources.OfType<DataInsertResource>().SingleOrDefault();

            if (dataInsertResource is null)
            {
                return Task.CompletedTask;
            }

            var statusUpdater = (string status) =>
            {
                resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                {
                    ResourceType = "Data-Insert",
                    State = status
                });
            };

            var logger = resourceLoggerService.GetLogger(dataInsertResource);

            _ = Task.Run(async () =>
            {
                if (dataInsertResource.TryGetAnnotationsOfType<WaitAnnotation>(out var waitAnnotations))
                {
                    await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                    {
                        ResourceType = "Data-Insert",
                        State = "Waiting"
                    });
                    List<Task> waitTasks = new List<Task>();
                    foreach (var r in waitAnnotations)
                    {
                        waitTasks.Add(resourceNotificationService.WaitForResourceHealthyAsync(r.Resource.Name, cancellationToken));
                    }
                    await Task.WhenAll(waitTasks);

                    logger.LogInformation("Inserting initial data");
                    await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                    {
                        ResourceType = "Data-Insert",
                        State = "Insert initial"
                    });
                    await dataInsertResource.initialInsert(logger, statusUpdater, tokenSource.Token);

                    await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                    {
                        ResourceType = "Data-Insert",
                        State = "Running"
                    });

                    await dataInsertResource.afterStart(logger, tokenSource.Token);

                    await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                    {
                        ResourceType = "Data-Insert",
                        State = "Finished"
                    });
                }
            });

            return Task.CompletedTask;
        }
        
        public ValueTask DisposeAsync()
        {
            tokenSource.Cancel();
            return ValueTask.CompletedTask;
        }
    }

    internal class DataInsertResource : Resource, IResourceWithWaitSupport
    {
        internal readonly Func<ILogger, Action<string>, CancellationToken, Task> initialInsert;
        internal readonly Func<ILogger, CancellationToken, Task> afterStart;
        public DataInsertResource(string name, Func<ILogger, Action<string>, CancellationToken, Task> initialInsert, Func<ILogger, CancellationToken, Task> afterStart) : base(name)
        {
            this.initialInsert = initialInsert;
            this.afterStart = afterStart;
        }

        public static IResourceBuilder<DataInsertResource> AddDataInsert(
            IDistributedApplicationBuilder builder, 
            string name,
            Func<ILogger, Action<string>, CancellationToken, Task> before,
            Func<ILogger, CancellationToken, Task> after
            )
        {
            var resource = new DataInsertResource(name, before, after);

            builder.Services.TryAddLifecycleHook<DataInsertLifecyclehook>();

            var res = builder.AddResource<DataInsertResource>(resource); 

            return res;
        }
    }
}
