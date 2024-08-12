using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.DependencyInjection.Internal;
using Microsoft.Extensions.DependencyInjection;
using System.ComponentModel.Design;

namespace FlowtideDotNet.DependencyInjection
{
    public static class FlowtideServiceCollectionExtensions
    {
        public static IFlowtideDIBuilder AddFlowtideStream(this IServiceCollection services, string name)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name, nameof(name));

            var builder = new FlowtideDIBuilder(name, services);
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
