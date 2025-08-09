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

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FlowtideDotNet.DependencyInjection.Internal
{
    internal class StreamWorkerService : BackgroundService, IHostedLifecycleService
    {
        private readonly string name;
        private readonly IServiceProvider serviceProvider;
        private Base.Engine.DataflowStream? _stream;

        public StreamWorkerService(string name, IServiceProvider serviceProvider)
        {
            this.name = name;
            this.serviceProvider = serviceProvider;
        }

        public Task StartedAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StartingAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StoppedAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public async Task StoppingAsync(CancellationToken cancellationToken)
        {
            if (_stream != null)
            {
                await _stream.StopAsync();
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _stream = serviceProvider.GetRequiredKeyedService<Base.Engine.DataflowStream>(name);
            await _stream.RunAsync();
        }
    }
}
