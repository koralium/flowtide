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

namespace FlowtideDotNet.AspNetCore
{
    internal class UiMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly UiMiddlewareState uiMiddlewareState;
        
        private sealed class PageCache
        {
            public PageCache(string contentType, byte[] bytes)
            {
                ContentType = contentType;
                Bytes = bytes;
            }

            public string ContentType { get; }
            public byte[] Bytes { get; }
        }

        private readonly Dictionary<string, PageCache> _pathCache;
        private string _comparePath;

        public UiMiddleware(RequestDelegate next, UiMiddlewareState uiMiddlewareState) 
        {
            _next = next;
            this.uiMiddlewareState = uiMiddlewareState;

            _comparePath = uiMiddlewareState.RootPath;
            if (_comparePath.EndsWith("/"))
            {
                _comparePath = _comparePath.Substring(0, _comparePath.Length - 1);
            }

            _pathCache = new Dictionary<string, PageCache>();
        }

        public Task Invoke(HttpContext httpContext)
        {
            if (httpContext == null)
                throw new ArgumentNullException(nameof(httpContext));

            var requestPath = httpContext.Request.Path.ToString();

            if (requestPath.StartsWith(_comparePath))
            {
                var remaining = requestPath.Substring(_comparePath.Length);
                if (remaining.Equals("/api/diagnostics"))
                {
                    return uiMiddlewareState.DiagnosticsEndpoint.Invoke(httpContext);
                }
                return uiMiddlewareState.ReactEndpoint.Invoke(httpContext);
            }
            else
            {
                return _next(httpContext);
            }
        }

        private string FormatToManifest(string path)
        {
            return path.Replace("/", ".");
        }

    }
}
