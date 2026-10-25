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

using Stowage;
using Stowage.Impl.Amazon;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Web;
using System.Xml.Linq;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public enum StoreFlavor
    {
        Azure,
        // An Azure account with a hierarchical namespace: directories are listed as entries and slash sorts lowest
        AzureHns,
        S3
    }

    /// <summary>
    /// An Azure Blob or S3 service on a loopback port for the real Stowage clients; it records every request line as sent.
    /// </summary>
    internal sealed class LoopbackObjectStore : IAsyncDisposable
    {
        public const string Container = "c";

        // Every object's time unless a test sets another, and every directory's
        public static readonly DateTimeOffset FixedTime = new DateTimeOffset(2009, 9, 9, 9, 20, 2, TimeSpan.Zero);

        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private readonly object _lock = new object();
        private readonly Dictionary<string, byte[]> _objects = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        private readonly Dictionary<string, DateTimeOffset> _modified = new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
        private DateTimeOffset _now = FixedTime;
        private readonly Dictionary<string, Dictionary<string, byte[]>> _uploads = new Dictionary<string, Dictionary<string, byte[]>>(StringComparer.Ordinal);
        private readonly Task _accept;
        private int _uploadIds;
        private int _listRequests;

        public LoopbackObjectStore(StoreFlavor flavor, string? endpointPath = null)
        {
            Flavor = flavor;
            EndpointPath = endpointPath ?? "";
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _accept = Task.Run(AcceptAsync);
        }

        public StoreFlavor Flavor { get; }

        public int Port { get; }

        // A path segment of the endpoint, as an emulator's account path
        public string EndpointPath { get; }

        public Uri Endpoint => new Uri($"http://127.0.0.1:{Port}/{EndpointPath}");

        // Request lines as received, in order
        public ConcurrentQueue<string> Requests { get; } = new ConcurrentQueue<string>();

        // Entries per listing page, by the listing request's number from 1
        public Func<int, int>? PageSize { get; set; }

        // A response that replaces the normal one, by method and raw target
        public Func<string, string, (int Status, string Body)?>? Intercept { get; set; }

        public IFileStorage CreateStorage()
        {
            return Flavor == StoreFlavor.S3
                ? Files.Of.AmazonS3("key", "secret", "us-east-1", Endpoint, null, BucketAddressingStyle.Path)
                : Files.Of.AzureBlobStorage(Endpoint, "account", Convert.ToBase64String(new byte[32]));
        }

        // The time every object write is stamped with, whole seconds as the services report them
        public DateTimeOffset Now
        {
            get
            {
                lock (_lock)
                {
                    return _now;
                }
            }
            set
            {
                lock (_lock)
                {
                    _now = value;
                }
            }
        }

        // An object's name within the container or bucket, stamped with Now unless a time is given
        public void Put(string name, byte[]? content = null, DateTimeOffset? modified = null)
        {
            lock (_lock)
            {
                Store(name, content ?? Encoding.UTF8.GetBytes(name));
                if (modified is { } time)
                {
                    _modified[name] = time;
                }
            }
        }

        // Under the lock
        private void Store(string name, byte[] content)
        {
            _objects[name] = content;
            _modified[name] = _now;
        }

        // Under the lock
        private bool Remove(string name)
        {
            _modified.Remove(name);
            return _objects.Remove(name);
        }

        // Under the lock
        private DateTimeOffset ModifiedOf(string name) => _modified.TryGetValue(name, out var time) ? time : FixedTime;

        private static string HttpDate(DateTimeOffset time) => time.UtcDateTime.ToString("r", CultureInfo.InvariantCulture);

        public bool Contains(string name)
        {
            lock (_lock)
            {
                return _objects.ContainsKey(name);
            }
        }

        public IReadOnlyList<string> Names
        {
            get
            {
                lock (_lock)
                {
                    return _objects.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();
                }
            }
        }

        private async Task AcceptAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(_stop.Token);
                }
                catch
                {
                    return;
                }
                _ = Task.Run(() => ServeAsync(client));
            }
        }

        private async Task ServeAsync(TcpClient client)
        {
            using (client)
            {
                var stream = client.GetStream();
                try
                {
                    while (true)
                    {
                        var head = await ReadHeadAsync(stream);
                        if (head == null)
                        {
                            return;
                        }
                        var lines = head.Split("\r\n");
                        var requestLine = lines[0];
                        Requests.Enqueue(requestLine);
                        var headers = lines.Skip(1).Where(x => x.Contains(':')).ToDictionary(x => x[..x.IndexOf(':')].Trim(), x => x[(x.IndexOf(':') + 1)..].Trim(), StringComparer.OrdinalIgnoreCase);
                        var body = await ReadBodyAsync(stream, headers);
                        var parts = requestLine.Split(' ');
                        var response = Handle(parts[0], parts[1], headers, body);
                        await WriteAsync(stream, parts[0], response);
                        if (headers.TryGetValue("Connection", out var connection) && connection.Equals("close", StringComparison.OrdinalIgnoreCase))
                        {
                            return;
                        }
                    }
                }
                catch (IOException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        private static async Task<string?> ReadHeadAsync(NetworkStream stream)
        {
            var buffer = new List<byte>();
            var one = new byte[1];
            while (true)
            {
                var read = await stream.ReadAsync(one);
                if (read == 0)
                {
                    return null;
                }
                buffer.Add(one[0]);
                var n = buffer.Count;
                if (n >= 4 && buffer[n - 1] == 10 && buffer[n - 2] == 13 && buffer[n - 3] == 10 && buffer[n - 4] == 13)
                {
                    return Encoding.Latin1.GetString(buffer.ToArray(), 0, n - 4);
                }
            }
        }

        private static async Task<byte[]> ReadBodyAsync(NetworkStream stream, Dictionary<string, string> headers)
        {
            if (headers.TryGetValue("Content-Length", out var lengthText) && long.TryParse(lengthText, out var length))
            {
                var body = new byte[length];
                var read = 0;
                while (read < length)
                {
                    var n = await stream.ReadAsync(body.AsMemory(read));
                    if (n == 0)
                    {
                        break;
                    }
                    read += n;
                }
                return body;
            }
            if (headers.TryGetValue("Transfer-Encoding", out var encoding) && encoding.Contains("chunked", StringComparison.OrdinalIgnoreCase))
            {
                var body = new MemoryStream();
                while (true)
                {
                    var sizeLine = await ReadLineAsync(stream);
                    var size = int.Parse(sizeLine.Split(';')[0], NumberStyles.HexNumber);
                    if (size == 0)
                    {
                        await ReadLineAsync(stream);
                        return body.ToArray();
                    }
                    var chunk = new byte[size];
                    var read = 0;
                    while (read < size)
                    {
                        var n = await stream.ReadAsync(chunk.AsMemory(read));
                        if (n == 0)
                        {
                            throw new EndOfStreamException("The client closed the connection inside a chunk");
                        }
                        read += n;
                    }
                    body.Write(chunk);
                    await ReadLineAsync(stream);
                }
            }
            return Array.Empty<byte>();
        }

        private static async Task<string> ReadLineAsync(NetworkStream stream)
        {
            var buffer = new List<byte>();
            var one = new byte[1];
            while (await stream.ReadAsync(one) > 0)
            {
                if (one[0] == 10)
                {
                    break;
                }
                if (one[0] != 13)
                {
                    buffer.Add(one[0]);
                }
            }
            return Encoding.ASCII.GetString(buffer.ToArray());
        }

        private sealed record Response(int Status, byte[] Body, Dictionary<string, string> Headers);

        private static Response Text(int status, string body, string contentType = "application/xml")
        {
            return new Response(status, Encoding.UTF8.GetBytes(body), new Dictionary<string, string>() { ["Content-Type"] = contentType });
        }

        private static async Task WriteAsync(NetworkStream stream, string method, Response response)
        {
            var reason = response.Status switch { 200 => "OK", 201 => "Created", 202 => "Accepted", 204 => "No Content", 400 => "Bad Request", 403 => "Forbidden", 404 => "Not Found", _ => "Error" };
            var head = new StringBuilder($"HTTP/1.1 {response.Status} {reason}\r\n");
            foreach (var (key, value) in response.Headers)
            {
                head.Append($"{key}: {value}\r\n");
            }
            if (!response.Headers.ContainsKey("Content-Length"))
            {
                head.Append($"Content-Length: {(method == "HEAD" ? 0 : response.Body.Length)}\r\n");
            }
            head.Append("x-ms-request-id: 1\r\nx-amz-request-id: 1\r\n\r\n");
            await stream.WriteAsync(Encoding.ASCII.GetBytes(head.ToString()));
            if (method != "HEAD")
            {
                await stream.WriteAsync(response.Body);
            }
            await stream.FlushAsync();
        }

        // The service's view of a target: the path percent-decoded, query values form-decoded as Azure and AWS read them
        private (string Path, Dictionary<string, string?> Query) Parse(string target)
        {
            var queryStart = target.IndexOf('?');
            var rawPath = queryStart < 0 ? target : target[..queryStart];
            var query = new Dictionary<string, string?>(StringComparer.Ordinal);
            if (queryStart >= 0)
            {
                foreach (var part in target[(queryStart + 1)..].Split('&', StringSplitOptions.RemoveEmptyEntries))
                {
                    var eq = part.IndexOf('=');
                    var key = HttpUtility.UrlDecode(eq < 0 ? part : part[..eq]);
                    query[key] = eq < 0 ? null : HttpUtility.UrlDecode(part[(eq + 1)..]);
                }
            }
            var path = Uri.UnescapeDataString(rawPath);
            if (EndpointPath.Length > 0)
            {
                // A request without the endpoint's path segment addresses another account
                var account = "/" + EndpointPath.TrimEnd('/');
                path = path.StartsWith(account + "/", StringComparison.Ordinal) || path == account ? path[account.Length..] : "/<other-account>" + path;
            }
            return (path, query);
        }

        private Response Handle(string method, string target, Dictionary<string, string> headers, byte[] body)
        {
            if (Intercept?.Invoke(method, target) is { } intercepted)
            {
                return Text(intercepted.Status, intercepted.Body);
            }
            var (path, query) = Parse(target);
            var trimmed = path.TrimStart('/');
            var slash = trimmed.IndexOf('/');
            var container = slash < 0 ? trimmed : trimmed[..slash];
            var name = slash < 0 ? "" : trimmed[(slash + 1)..];
            if (container != Container)
            {
                return Text(404, "<Error><Code>ContainerNotFound</Code></Error>");
            }
            lock (_lock)
            {
                return Flavor == StoreFlavor.S3 ? HandleS3(method, name, query, body) : HandleAzure(method, name, query, body);
            }
        }

        private int NextPageSize(int fallback)
        {
            var number = Interlocked.Increment(ref _listRequests);
            return PageSize?.Invoke(number) ?? fallback;
        }

        // S3's UTF-8 byte order, Azure's ordinal order, or the hierarchical order where a slash sorts below every other character
        private IEnumerable<string> Ordered(IEnumerable<string> names)
        {
            return Flavor switch
            {
                StoreFlavor.AzureHns => names.OrderBy(x => x.Replace('/', '\u0001'), StringComparer.Ordinal),
                StoreFlavor.S3 => names.OrderBy(x => Encoding.UTF8.GetBytes(x), Comparer<byte[]>.Create((left, right) => left.AsSpan().SequenceCompareTo(right))),
                _ => names.OrderBy(x => x, StringComparer.Ordinal)
            };
        }

        // Every object and, on a hierarchical account, every directory below the prefix
        private List<(string Name, bool Directory)> Listable(string prefix)
        {
            var entries = _objects.Keys.Where(x => x.StartsWith(prefix, StringComparison.Ordinal)).Select(x => (x, false)).ToList();
            if (Flavor == StoreFlavor.AzureHns)
            {
                var directories = new HashSet<string>(StringComparer.Ordinal);
                foreach (var (name, _) in entries)
                {
                    var at = name.IndexOf('/', prefix.Length);
                    while (at >= 0)
                    {
                        directories.Add(name[..at]);
                        at = name.IndexOf('/', at + 1);
                    }
                }
                entries.AddRange(directories.Where(x => x.Length > prefix.Length || !prefix.EndsWith('/')).Select(x => (x, true)));
            }
            var directorySet = entries.Where(x => x.Item2).Select(x => x.Item1).ToHashSet(StringComparer.Ordinal);
            return Ordered(entries.Select(x => x.Item1)).Select(x => (x, directorySet.Contains(x))).ToList();
        }

        private Response HandleAzure(string method, string name, Dictionary<string, string?> query, byte[] body)
        {
            if (method == "GET" && query.TryGetValue("comp", out var comp) && comp == "list")
            {
                return ListAzure(query);
            }
            switch (method)
            {
                case "PUT":
                    if (query.TryGetValue("comp", out var putComp) && putComp == "block")
                    {
                        Upload(name)[query["blockid"]!] = body;
                        return Text(201, "");
                    }
                    if (query.TryGetValue("comp", out putComp) && putComp == "blocklist")
                    {
                        var ids = XDocument.Parse(Encoding.UTF8.GetString(body)).Root!.Elements().Select(x => x.Value).ToList();
                        var blocks = Upload(name);
                        Store(name, ids.SelectMany(id => blocks[id]).ToArray());
                        _uploads.Remove(name);
                        return Text(201, "");
                    }
                    Store(name, body);
                    return Text(201, "");
                case "GET":
                    return _objects.TryGetValue(name, out var content)
                        ? new Response(200, content, new Dictionary<string, string>() { ["Content-Type"] = "application/octet-stream", ["Last-Modified"] = HttpDate(ModifiedOf(name)), ["ETag"] = "\"0x1\"" })
                        : Text(404, "<Error><Code>BlobNotFound</Code></Error>");
                case "HEAD":
                    return _objects.TryGetValue(name, out var head)
                        ? new Response(200, Array.Empty<byte>(), new Dictionary<string, string>() { ["Content-Length"] = head.Length.ToString(CultureInfo.InvariantCulture), ["Last-Modified"] = HttpDate(ModifiedOf(name)), ["ETag"] = "\"0x1\"", ["x-ms-blob-type"] = "BlockBlob" })
                        : Text(404, "");
                case "DELETE":
                    return Remove(name) ? Text(202, "") : Text(404, "<Error><Code>BlobNotFound</Code></Error>");
            }
            return Text(400, "<Error><Code>UnsupportedHttpVerb</Code></Error>");
        }

        private Dictionary<string, byte[]> Upload(string name)
        {
            if (!_uploads.TryGetValue(name, out var blocks))
            {
                blocks = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                _uploads[name] = blocks;
            }
            return blocks;
        }

        private Response ListAzure(Dictionary<string, string?> query)
        {
            var prefix = query.GetValueOrDefault("prefix") ?? "";
            var delimiter = query.GetValueOrDefault("delimiter");
            var marker = query.GetValueOrDefault("marker");
            var size = NextPageSize(5000);
            // The marker names the first entry of the next page
            var all = Listable(prefix);
            var items = new List<(string Name, bool Directory, bool Prefix)>();
            var seenPrefixes = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (name, directory) in all)
            {
                if (!string.IsNullOrEmpty(delimiter))
                {
                    var at = name.IndexOf(delimiter, prefix.Length, StringComparison.Ordinal);
                    if (at >= 0)
                    {
                        var commonPrefix = name[..(at + delimiter.Length)];
                        if (seenPrefixes.Add(commonPrefix))
                        {
                            items.Add((commonPrefix, false, true));
                        }
                        continue;
                    }
                }
                items.Add((name, directory, false));
            }
            var start = 0;
            if (!string.IsNullOrEmpty(marker))
            {
                var resume = MarkerName(marker);
                start = resume == null ? -1 : items.FindIndex(x => x.Name == resume);
            }
            if (start < 0)
            {
                return Text(400, "<Error><Code>InvalidQueryParameterValue</Code></Error>");
            }
            var page = items.Skip(start).Take(size).ToList();
            var next = start + size < items.Count ? Marker(items[start + size].Name) : "";
            var blobs = new XElement("Blobs");
            foreach (var item in page)
            {
                if (item.Prefix)
                {
                    blobs.Add(new XElement("BlobPrefix", new XElement("Name", item.Name)));
                    continue;
                }
                var properties = new XElement("Properties",
                    new XElement("Last-Modified", HttpDate(item.Directory ? FixedTime : ModifiedOf(item.Name))),
                    new XElement("Etag", "0x8CBFF45D8A29A19"),
                    new XElement("Content-Length", item.Directory ? 0 : _objects[item.Name].Length),
                    new XElement("Content-Type", "application/octet-stream"),
                    new XElement("BlobType", "BlockBlob"));
                if (Flavor == StoreFlavor.AzureHns)
                {
                    properties.Add(new XElement("ResourceType", item.Directory ? "directory" : "file"));
                }
                blobs.Add(new XElement("Blob", new XElement("Name", item.Name), properties));
            }
            var root = new XElement("EnumerationResults", new XAttribute("ServiceEndpoint", Endpoint.ToString()), new XAttribute("ContainerName", Container),
                new XElement("Prefix", prefix), blobs, new XElement("NextMarker", next));
            return Text(200, "<?xml version=\"1.0\" encoding=\"utf-8\"?>" + root.ToString(SaveOptions.DisableFormatting));
        }

        /// <summary>
        /// A marker in the observed public format: 2!length!base64 of the next name and a fixed tail, padding written as '-'.
        /// </summary>
        public static string Marker(string name)
        {
            var payload = $"{name.Length:D6}!{name}!000028!9999-12-31T23:59:59.9999999Z!";
            var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(payload)).Replace('=', '-');
            return $"2!{encoded.Length}!{encoded}";
        }

        // The name a marker resumes at, null when it is not one
        private static string? MarkerName(string marker)
        {
            var parts = marker.Split('!', 3);
            if (parts.Length != 3 || parts[0] != "2" || !int.TryParse(parts[1], out var length) || length != parts[2].Length)
            {
                return null;
            }
            try
            {
                var payload = Encoding.UTF8.GetString(Convert.FromBase64String(parts[2].Replace('-', '=')));
                var fields = payload.Split('!');
                return fields.Length >= 2 && int.TryParse(fields[0], out var nameLength) && nameLength == fields[1].Length ? fields[1] : null;
            }
            catch (FormatException)
            {
                return null;
            }
        }

        private Response HandleS3(string method, string name, Dictionary<string, string?> query, byte[] body)
        {
            if (method == "GET" && name.Length == 0 && query.TryGetValue("list-type", out var listType) && listType == "2")
            {
                return ListS3(query);
            }
            if (method == "POST" && query.ContainsKey("uploads"))
            {
                var uploadId = $"upload-{Interlocked.Increment(ref _uploadIds)}";
                _uploads[uploadId] = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                return Text(200, $"<InitiateMultipartUploadResult><Bucket>{Container}</Bucket><Key>{SecurityElement(name)}</Key><UploadId>{uploadId}</UploadId></InitiateMultipartUploadResult>");
            }
            if (method == "PUT" && query.TryGetValue("uploadId", out var partUpload) && query.TryGetValue("partNumber", out var partNumber))
            {
                _uploads[partUpload!][partNumber!] = body;
                return new Response(200, Array.Empty<byte>(), new Dictionary<string, string>() { ["ETag"] = $"\"etag-{partNumber}\"" });
            }
            if (method == "POST" && query.TryGetValue("uploadId", out var completeUpload))
            {
                var parts = _uploads[completeUpload!];
                Store(name, parts.OrderBy(x => int.Parse(x.Key, CultureInfo.InvariantCulture)).SelectMany(x => x.Value).ToArray());
                _uploads.Remove(completeUpload!);
                return Text(200, $"<CompleteMultipartUploadResult><Bucket>{Container}</Bucket><Key>{SecurityElement(name)}</Key><ETag>\"x\"</ETag></CompleteMultipartUploadResult>");
            }
            switch (method)
            {
                case "PUT":
                    Store(name, body);
                    return new Response(200, Array.Empty<byte>(), new Dictionary<string, string>() { ["ETag"] = "\"x\"" });
                case "GET":
                    return _objects.TryGetValue(name, out var content)
                        ? new Response(200, content, new Dictionary<string, string>() { ["Content-Type"] = "application/octet-stream", ["Last-Modified"] = HttpDate(ModifiedOf(name)), ["ETag"] = "\"x\"" })
                        : Text(404, "<Error><Code>NoSuchKey</Code></Error>");
                case "HEAD":
                    return _objects.TryGetValue(name, out var head)
                        ? new Response(200, Array.Empty<byte>(), new Dictionary<string, string>() { ["Content-Length"] = head.Length.ToString(CultureInfo.InvariantCulture), ["Last-Modified"] = HttpDate(ModifiedOf(name)), ["ETag"] = "\"x\"" })
                        : Text(404, "");
                case "DELETE":
                    Remove(name);
                    return Text(204, "");
            }
            return Text(400, "<Error><Code>NotImplemented</Code></Error>");
        }

        private static string SecurityElement(string text) => System.Security.SecurityElement.Escape(text) ?? "";

        // Continuation tokens carry the characters a query string reserves
        public const string TokenPrefix = "1a+b/c=d&e%";

        private Response ListS3(Dictionary<string, string?> query)
        {
            var prefix = query.GetValueOrDefault("prefix") ?? "";
            var delimiter = query.GetValueOrDefault("delimiter");
            var token = query.GetValueOrDefault("continuation-token");
            var size = NextPageSize(1000);
            var items = new List<(string Name, bool Prefix)>();
            var seenPrefixes = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (name, _) in Listable(prefix))
            {
                if (!string.IsNullOrEmpty(delimiter))
                {
                    var at = name.IndexOf(delimiter, prefix.Length, StringComparison.Ordinal);
                    if (at >= 0)
                    {
                        if (seenPrefixes.Add(name[..(at + delimiter.Length)]))
                        {
                            items.Add((name[..(at + delimiter.Length)], true));
                        }
                        continue;
                    }
                }
                items.Add((name, false));
            }
            var start = 0;
            if (!string.IsNullOrEmpty(token))
            {
                // Real tokens are opaque base64 with reserved characters, a damaged one is rejected
                if (!token.StartsWith(TokenPrefix, StringComparison.Ordinal) || !int.TryParse(token.AsSpan(TokenPrefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out start))
                {
                    return Text(400, "<Error><Code>InvalidArgument</Code></Error>");
                }
            }
            var page = items.Skip(start).Take(size).ToList();
            var truncated = start + size < items.Count;
            var root = new XElement("ListBucketResult",
                new XElement("Name", Container),
                new XElement("Prefix", prefix),
                new XElement("KeyCount", page.Count),
                new XElement("MaxKeys", size),
                new XElement("IsTruncated", truncated ? "true" : "false"));
            if (truncated)
            {
                root.Add(new XElement("NextContinuationToken", TokenPrefix + (start + size).ToString(CultureInfo.InvariantCulture)));
            }
            foreach (var item in page.Where(x => !x.Prefix))
            {
                root.Add(new XElement("Contents",
                    new XElement("Key", item.Name),
                    new XElement("LastModified", ModifiedOf(item.Name).UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)),
                    new XElement("ETag", "\"x\""),
                    new XElement("Size", _objects[item.Name].Length),
                    new XElement("StorageClass", "STANDARD")));
            }
            foreach (var item in page.Where(x => x.Prefix))
            {
                root.Add(new XElement("CommonPrefixes", new XElement("Prefix", item.Name)));
            }
            return Text(200, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + root.ToString(SaveOptions.DisableFormatting));
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                await _accept;
            }
            catch
            {
            }
        }
    }
}
