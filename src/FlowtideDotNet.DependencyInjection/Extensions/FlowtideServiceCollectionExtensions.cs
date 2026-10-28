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

using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.DependencyInjection.Internal;
using FlowtideDotNet.Substrait.Sql;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class FlowtideServiceCollectionExtensions
    {
        /// <summary>
        /// Registers a named stream that continuously maintains query results as source data changes.
        /// Use the returned builder to configure its plan, source and sink connectors, and state storage.
        /// </summary>
        /// <param name="services">The application service collection.</param>
        /// <param name="name">A stable stream name used to identify its services and persisted state.</param>
        /// <returns>A builder for configuring the plan, connectors and state storage.</returns>
        /// <remarks>
        /// <para>
        /// A stream needs a plan, source and sink connectors, and state storage.
        /// Configure these on the returned builder before starting the application host:
        /// </para>
        /// <list type="number">
        /// <item>
        /// <term>Plan</term>
        /// <description>
        /// Use <see cref="FlowtideDIBuilderExtensions.AddSqlTextAsPlan"/> for inline SQL,
        /// <see cref="FlowtideDIBuilderExtensions.AddSqlFileAsPlan"/> for a SQL file, or
        /// <see cref="FlowtideDIBuilderExtensions.AddSqlPlan"/> to configure a SQL plan builder.
        /// The query describes the result to maintain; <c>INSERT INTO</c> names its destination.
        /// </description>
        /// </item>
        /// <item>
        /// <term>Source and sink connectors</term>
        /// <description>
        /// Use <see cref="IFlowtideDIBuilder.AddConnectors"/> to register sources for the input tables
        /// and sinks for the destinations named in the plan. Install the connector packages for your data systems
        /// and make sure their table names match the SQL. Built-in inputs such as <c>VALUES</c> need no source connector.
        /// </description>
        /// </item>
        /// <item>
        /// <term>State storage</term>
        /// <description>
        /// Use <see cref="IFlowtideDIBuilder.AddStorage"/> to store operator state, source positions and checkpoints.
        /// Choose <see cref="FlowtideStorageExtensions.AddTemporaryStorage"/> for development and tests, or
        /// <see cref="FlowtideStorageExtensions.AddFileStorage"/> for local state that survives application restarts.
        /// </description>
        /// </item>
        /// </list>
        /// <para>
        /// The application host starts and stops the stream. Calling this method registers its services;
        /// run the host after completing the configuration, as shown in the example.
        /// Give independent streams their own names and storage directories. Calling this method again
        /// with the same name on the same service collection returns the existing builder.
        /// </para>
        /// </remarks>
        /// <example>
        /// <para>
        /// A complete console application using the .NET Generic Host. Install the
        /// FlowtideDotNet.DependencyInjection and Microsoft.Extensions.Hosting NuGet packages.
        /// This example uses a built-in VALUES source and the console sink, so no external database is needed.
        /// </para>
        /// <code language="csharp"><![CDATA[
        /// using System.IO;
        /// using FlowtideDotNet.Core.Sinks;
        /// using FlowtideDotNet.DependencyInjection;
        /// using Microsoft.Extensions.DependencyInjection;
        /// using Microsoft.Extensions.Hosting;
        ///
        /// var builder = Host.CreateApplicationBuilder(args);
        ///
        /// builder.Services.AddFlowtideStream("hello")
        ///     .AddSqlTextAsPlan("""
        ///         INSERT INTO console
        ///         SELECT id, upper(name) AS name
        ///         FROM (VALUES (1, 'Ada'), (2, 'Linus')) AS people(id, name);
        ///         """)
        ///     .AddConnectors(connectors => connectors.AddConsoleSink("console"))
        ///     .AddStorage(storage =>
        ///     {
        ///         storage.AddTemporaryStorage(Path.Combine(Path.GetTempPath(), "flowtide-hello"));
        ///     });
        ///
        /// using var host = builder.Build();
        /// await host.RunAsync();
        /// ]]></code>
        /// <para>
        /// Run the application to start the stream and print its results. The host handles startup and shutdown;
        /// press Ctrl+C to stop. Temporary storage is cleaned up on disposal. For state that survives restarts,
        /// replace the storage call with <c>storage.AddFileStorage("./state/hello")</c> and keep that directory.
        /// </para>
        /// </example>
        /// <seealso cref="SqlFunctionReference"/>
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
                return provider.GetRequiredKeyedService<FlowtideDotNet.Base.Engine.DataflowStream>(name);
            });
            services.AddHostedService(p => new StreamWorkerService(name, p));

            return builder;
        }
    }
}
