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

using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace FlowtideDotNet.AspNetCore.Extensions
{
    public static class UIApplicationBuilderExtensions
    {
        public static IApplicationBuilder UseFlowtideUI(this IApplicationBuilder app, string path = "/ui/stream")
        {
            var stream = app.ApplicationServices.GetRequiredService<FlowtideDotNet.Base.Engine.DataflowStream>();
            return app.UseFlowtideUI(stream, path);
        }

        public static IApplicationBuilder UseFlowtideUI(this IApplicationBuilder app, FlowtideDotNet.Base.Engine.DataflowStream dataflowStream, string path = "/ui/stream")
        {
            return app.UseMiddleware<UiMiddleware>(new UiMiddlewareState(new DiagnosticsEndpoint(dataflowStream), new ReactEndpoint(path), path));
        }

    }
}
