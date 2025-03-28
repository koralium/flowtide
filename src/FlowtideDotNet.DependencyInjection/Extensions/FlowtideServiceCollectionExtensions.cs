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

using FlowtideDotNet.DependencyInjection.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace FlowtideDotNet.DependencyInjection
{
    public static class FlowtideServiceCollectionExtensions
    {
        public static IFlowtideDIBuilder AddFlowtideStream(this IServiceCollection services, string name)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name, nameof(name));

            // Search services for an existing builder with the same key
            var existingBuilderObj = services.FirstOrDefault(x => 
                x.IsKeyedService && 
                (x.ServiceKey?.Equals(name) ?? false) && 
                x.ServiceType == typeof(FlowtideDIBuilder));
            
            if (existingBuilderObj != null && existingBuilderObj.KeyedImplementationInstance is FlowtideDIBuilder existingBuilder)
            {
                return existingBuilder;
            }

            var builder = new FlowtideDIBuilder(name, services);

            // Add the builder to DI so it can be fetched again.
            services.AddKeyedSingleton(name, builder);
            services.AddKeyedSingleton(name, (provider, key) =>
            {
                return builder.Build(provider);
            });
            // Add it without key as well because its required for the UI at this moment.
            services.AddSingleton(provider =>
            {
                return provider.GetRequiredKeyedService<Base.Engine.DataflowStream>(name);
            });
            services.AddHostedService(p => new StreamWorkerService(name, p));

            return builder;
        }
    }
}
