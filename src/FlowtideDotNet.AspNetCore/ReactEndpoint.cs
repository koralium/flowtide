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

namespace FlowtideDotNet.AspNetCore
{
    internal class ReactEndpoint
    {
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

        private readonly string m_rootPath;
        private readonly string m_apiPath;
        private readonly Dictionary<string, PageCache> _pathCache;

        public ReactEndpoint(string rootPath)
        {
            if (rootPath.EndsWith("/"))
            {
                rootPath = rootPath.Substring(rootPath.Length - 1);
            }
            this.m_rootPath = rootPath;

            m_apiPath = m_rootPath;
            if (!m_apiPath.EndsWith("/"))
            {
                m_apiPath += "/";
            }

            _pathCache = new Dictionary<string, PageCache>();
        }

        public Task Invoke(HttpContext httpContext)
        {
            httpContext.Response.StatusCode = 200;

            var requestPath = httpContext.Request.Path.ToString();

            if (_pathCache.TryGetValue(requestPath, out var cachedPage))
            {
                httpContext.Response.ContentType = cachedPage.ContentType;
                return httpContext.Response.Body.WriteAsync(cachedPage.Bytes, 0, cachedPage.Bytes.Length);
            }

            var remain = requestPath.Substring(m_rootPath.Length);

            if (remain.StartsWith("/"))
            {
                remain = remain.Substring(1);
            }
            if (string.IsNullOrEmpty(remain))
            {
                remain = "index.html";
            }
            else
            {
                remain = FormatToManifest(remain);
            }

            bool isHtml = false;
            bool isText = false;
            if (remain.EndsWith(".html"))
            {
                httpContext.Response.ContentType = "text/html";
                isText = true;
                isHtml = true;
            }
            else if (remain.EndsWith(".js"))
            {
                httpContext.Response.ContentType = "text/javascript";
                isText = true;
            }
            else if (remain.EndsWith(".png"))
            {
                httpContext.Response.ContentType = "image/png";
            }
            else if (remain.EndsWith(".svg"))
            {
                httpContext.Response.ContentType = "image/svg+xml";
            }
            else if (remain.EndsWith(".css"))
            {
                httpContext.Response.ContentType = "text/css";
                isText = true;
            }
            else if (remain.EndsWith(".json"))
            {
                httpContext.Response.ContentType = "application/json";
                isText = true;
            }

            using var stream = typeof(ReactEndpoint).Assembly
                .GetManifestResourceStream($"FlowtideDotNet.AspNetCore.ClientApp.build.{remain}")!;

            if (isText)
            {
                using var streamReader = new StreamReader(stream);
                var txt = streamReader.ReadToEnd();
                var combinedPath = Path.Combine(m_rootPath, "static/").Replace('\\', '/');

                if (combinedPath.StartsWith("/"))
                {
                    combinedPath = combinedPath.Substring(1);
                }
                txt = txt.Replace("static/", combinedPath);
                txt = txt.Replace("@(rootpath)", m_apiPath);
                if (isHtml)
                {
                    txt = txt.Replace("manifest.json", $"{m_rootPath.Substring(1)}manifest.json");
                }
                byte[] outputBytes = Encoding.UTF8.GetBytes(txt);

                _pathCache.Add(requestPath, new PageCache(httpContext.Response.ContentType!, outputBytes));
                return httpContext.Response.Body.WriteAsync(outputBytes, 0, outputBytes.Length);
            }
            else
            {
                using MemoryStream memoryStream = new MemoryStream();
                stream.CopyTo(memoryStream);
                var bytes = memoryStream.ToArray();

                _pathCache.Add(requestPath, new PageCache(httpContext.Response.ContentType!, bytes));
                return httpContext.Response.Body.WriteAsync(bytes, 0, bytes.Length);
            }
        }

        private string FormatToManifest(string path)
        {
            return path.Replace("/", ".");
        }
    }
}
