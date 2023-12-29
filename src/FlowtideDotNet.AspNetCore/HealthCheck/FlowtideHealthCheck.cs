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

using FlowtideDotNet.Base.Engine;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace FlowtideDotNet.AspNetCore.HealthCheck
{
    internal class FlowtideHealthCheck : IHealthCheck
    {
        private readonly Base.Engine.DataflowStream dataflowStream;

        public FlowtideHealthCheck(Base.Engine.DataflowStream dataflowStream)
        {
            this.dataflowStream = dataflowStream;
        }
        public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var status = dataflowStream.Status;

            switch (status)
            {
                case StreamStatus.Failing:
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Unhealthy, "Stream is in state 'failing'."));
                case StreamStatus.Running:
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));
                case StreamStatus.Starting:
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Degraded, "Stream is in state 'starting'."));
                case StreamStatus.Stopped:
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Unhealthy, "Stream is in state 'stopped'."));
                case StreamStatus.Degraded:
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Degraded, "Stream is in state 'degraded'."));
                default:
                    return Task.FromResult(new HealthCheckResult(HealthStatus.Degraded, "Stream is in unknown state."));
            }
        }
    }
}
