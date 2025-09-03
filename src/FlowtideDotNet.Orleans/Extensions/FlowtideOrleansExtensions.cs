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

using FlowtideDotNet.Core;
using FlowtideDotNet.Orleans.Internal;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Serializers;
using Orleans.Serialization;
using FlowtideDotNet.DependencyInjection;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class FlowtideOrleansExtensions
    {
        public static IServiceCollection AddFlowtideOrleans(this IServiceCollection services, 
            Action<IConnectorManager> connectors,
            Action<string, string, IFlowtideStorageBuilder> storageBuilder)
        {
            var connMgr = new ConnectorManager();
            connectors(connMgr);

            services.AddSingleton<OrleansPlanSerializer>();
            services.AddSingleton<IGeneralizedCodec, OrleansPlanSerializer>();
            services.AddSingleton<IGeneralizedCopier, OrleansPlanSerializer>();
            services.AddSingleton<ITypeFilter, OrleansPlanSerializer>();
            services.AddSingleton<IConnectorManager>(connMgr);

            services.AddSingleton<Action<string, string, IFlowtideStorageBuilder>>(storageBuilder);
            
            return services;
        }
    }
}
