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

using FlowtideDotNet.Core.Engine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.AspNetCore.Extensions
{
    public static class FlowtideServiceCollectionExtensions
    {
        [Obsolete("Use AddFlowtideStream(string name) instead.")]
        public static IServiceCollection AddFlowtideStream(this IServiceCollection services, Action<FlowtideBuilder> builder, string name = "stream")
        {
            FlowtideBuilder differentialComputeBuilder = new FlowtideBuilder(name);
            builder?.Invoke(differentialComputeBuilder);

            services.AddSingleton(differentialComputeBuilder);
            services.AddSingleton(provider =>
            {
                var b = provider.GetRequiredService<FlowtideBuilder>();
                var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
                b.WithLoggerFactory(loggerFactory);
                return b.Build();
            });
            services.AddHostedService<StreamHostedService>();
            return services;
        }
    }
}
