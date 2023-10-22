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

using Microsoft.AspNetCore.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.AspNetCore
{
    internal class DiagnosticsEndpoint
    {
        private FlowtideDotNet.Base.Engine.DataflowStream m_stream;
        private byte[]? m_cachedBytes;
        private DateTime m_cacheStored;

        public DiagnosticsEndpoint(FlowtideDotNet.Base.Engine.DataflowStream stream)
        {
            m_cacheStored = DateTime.MinValue;
            m_stream = stream;
        }

        public Task Invoke(HttpContext httpContext)
        {
            if (m_cachedBytes != null && m_cacheStored.CompareTo(DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(1))) > 0)
            {
                httpContext.Response.StatusCode = 200;
                httpContext.Response.ContentType = "application/json";
                return httpContext.Response.Body.WriteAsync(m_cachedBytes, 0, m_cachedBytes.Length);
            }

            var graph = m_stream.GetDiagnosticsGraph();
            var str = JsonSerializer.Serialize(graph, new JsonSerializerOptions()
            {
                WriteIndented = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                Converters =
                {
                    new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
                }
            });
            var diagnosticsBytes = Encoding.UTF8.GetBytes(str);
            m_cachedBytes = diagnosticsBytes;
            m_cacheStored = DateTime.UtcNow;
            httpContext.Response.StatusCode = 200;
            httpContext.Response.ContentType = "application/json";
            return httpContext.Response.Body.WriteAsync(diagnosticsBytes, 0, diagnosticsBytes.Length);
        }
    }
}
