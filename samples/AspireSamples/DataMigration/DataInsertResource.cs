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

using Aspire.Hosting.Eventing;
using Aspire.Hosting.Lifecycle;
using Microsoft.Extensions.Logging;

namespace AspireSamples.DataMigration
{
    internal class DataInsertEventingSubscriber : IDistributedApplicationEventingSubscriber
    {
        private readonly ResourceNotificationService resourceNotificationService;
        private readonly ResourceLoggerService resourceLoggerService;
        private CancellationTokenSource tokenSource;

        public DataInsertEventingSubscriber(
            ResourceNotificationService resourceNotificationService,
            ResourceLoggerService resourceLoggerService)
        {
            this.resourceNotificationService = resourceNotificationService;
            this.resourceLoggerService = resourceLoggerService;
            tokenSource = new CancellationTokenSource();
        }

        public Task SubscribeAsync(IDistributedApplicationEventing eventing, DistributedApplicationExecutionContext executionContext, CancellationToken cancellationToken)
        {
            eventing.Subscribe<BeforeStartEvent>(async (beforeStartEvent, c) =>
            {
                var dataInsertResource = beforeStartEvent.Model.Resources.OfType<DataInsertResource>().SingleOrDefault();

                if (dataInsertResource is null)
                {
                    return;
                }

                await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                {
                    ResourceType = "Data-Insert",
                    State = "Starting"
                });

                await eventing.PublishAsync<BeforeResourceStartedEvent>(new BeforeResourceStartedEvent(dataInsertResource, executionContext.ServiceProvider));
            });

            eventing.Subscribe<BeforeResourceStartedEvent>((resourceEndpointsAllocatedEvent, token) =>
            {
                if (!(resourceEndpointsAllocatedEvent.Resource is DataInsertResource dataInsertResource))
                {
                    return Task.CompletedTask;
                }

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
                        try
                        {
                            await dataInsertResource.initialInsert(logger, statusUpdater, dataInsertResource, tokenSource.Token);
                        }
                        catch (Exception e)
                        {
                            logger.LogError(e, "Error inserting initial data");
                            await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                            {
                                ResourceType = "Data-Insert",
                                State = "Failed"
                            });
                            return;
                        }


                        await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                        {
                            ResourceType = "Data-Insert",
                            State = "Running"
                        });

                        await dataInsertResource.afterStart(logger, dataInsertResource, tokenSource.Token);

                        await resourceNotificationService.PublishUpdateAsync(dataInsertResource, s => s with
                        {
                            ResourceType = "Data-Insert",
                            State = "Finished"
                        });
                    }
                });

                return Task.CompletedTask;
            });

            return Task.CompletedTask;
        }
    }

    internal class DataInsertResource : Resource, IResourceWithWaitSupport, IResourceWithEnvironment
    {
        internal readonly Func<ILogger, Action<string>, DataInsertResource, CancellationToken, Task> initialInsert;
        internal readonly Func<ILogger, DataInsertResource, CancellationToken, Task> afterStart;
        public DataInsertResource(string name, Func<ILogger, Action<string>, DataInsertResource, CancellationToken, Task> initialInsert, Func<ILogger, DataInsertResource, CancellationToken, Task> afterStart) : base(name)
        {
            this.initialInsert = initialInsert;
            this.afterStart = afterStart;
        }

        public static IResourceBuilder<DataInsertResource> AddDataInsert(
            IDistributedApplicationBuilder builder,
            string name,
            Func<ILogger, Action<string>, DataInsertResource, CancellationToken, Task> before,
            Func<ILogger, DataInsertResource, CancellationToken, Task> after
            )
        {
            var resource = new DataInsertResource(name, before, after);

            builder.Services.AddEventingSubscriber<DataInsertEventingSubscriber>();

            var res = builder.AddResource<DataInsertResource>(resource);

            return res;
        }
    }
}
