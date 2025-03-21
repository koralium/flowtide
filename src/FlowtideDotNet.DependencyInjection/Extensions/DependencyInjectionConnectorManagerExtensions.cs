using FlowtideDotNet.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class DependencyInjectionConnectorManagerExtensions
    {
        public static IDependencyInjectionConnectorManager AddOptionsSource<TOptions>(this IDependencyInjectionConnectorManager connectorManager, string tableName)
        {
            connectorManager.AddOptionsSource(tableName, connectorManager.ServiceProvider.GetRequiredService<IOptionsMonitor<TOptions>>());
            return connectorManager;
        }
    }
}
