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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Optimizer;
using Microsoft.Extensions.DependencyInjection;

namespace FlowtideDotNet.DependencyInjection
{
    public interface IFlowtideDIBuilder
    {
        string StreamName { get; }

        IServiceCollection Services { get; }

        IFlowtideDIBuilder AddConnectors(Action<IDependencyInjectionConnectorManager> registerFunc);

        IFlowtideDIBuilder SetPlanProvider(IFlowtidePlanProvider planProvider);

        IFlowtideDIBuilder SetPlanProvider<TPlanProvider>() where TPlanProvider : class, IFlowtidePlanProvider;

        IFlowtideDIBuilder AddStorage(Action<IFlowtideStorageBuilder> storageOptions);

        IFlowtideDIBuilder AddCustomOptions(Action<IServiceProvider, FlowtideBuilder> options);

        /// <summary>
        /// Sets the current entry assembly version as the stream version. A new version is created if the assembly version changes.
        /// Multiple calls to versioning methods (<see cref="AddVersioningFromAssembly"/>, <see cref="AddVersioningFromString(string)"/>, <see cref="AddVersioningFromPlanHash"/>) 
        /// will combine the version sources, creating a new version if any of them change.
        /// </summary>
        IFlowtideDIBuilder AddVersioningFromAssembly();

        /// <summary>
        /// Sets a custom string as the stream version. A new version is created if the string changes.
        /// Multiple calls to versioning methods (<see cref="AddVersioningFromAssembly"/>, <see cref="AddVersioningFromString(string)"/>, <see cref="AddVersioningFromPlanHash"/>) 
        /// will combine the version sources, creating a new version if any of them change.
        /// </summary>
        /// <param name="version"></param>
        IFlowtideDIBuilder AddVersioningFromString(string version);

        /// <summary>
        /// Sets the plan hash as the stream version. A new version is created if the plan changes.
        /// Multiple calls to versioning methods (<see cref="AddVersioningFromAssembly"/>, <see cref="AddVersioningFromString(string)"/>, <see cref="AddVersioningFromPlanHash"/>) 
        /// will combine the version sources, creating a new version if any of them change.
        /// </summary>
        IFlowtideDIBuilder AddVersioningFromPlanHash();

        IFlowtideDIBuilder SetOptimizerSettings(PlanOptimizerSettings planOptimizerSettings);
    }
}
