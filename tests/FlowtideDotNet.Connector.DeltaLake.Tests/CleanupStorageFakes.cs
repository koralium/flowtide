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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using Stowage.Impl.Amazon;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // The pinned S3 signer hashes with one static SHA256 that throws under concurrent use, so classes driving it never run in parallel
    [CollectionDefinition(nameof(PinnedClientCollection))]
    public class PinnedClientCollection
    {
    }

    /// <summary>
    /// Real pinned clients built without a request, for classification.
    /// </summary>
    internal static class StoreFixtures
    {
        public static IFileStorage S3() => Files.Of.AmazonS3("key", "secret", "us-east-1", new Uri("http://127.0.0.1:9"), null, BucketAddressingStyle.Path);

        // An endpoint path such as an emulator's account segment
        public static IFileStorage Azure(string endpointPath = "") => Files.Of.AzureBlobStorage(new Uri($"http://127.0.0.1:9/{endpointPath}"), "account", Convert.ToBase64String(new byte[32]));

        public static IFileStorage Gcs()
        {
            using var rsa = RSA.Create(2048);
            var credential = JsonSerializer.Serialize(new Dictionary<string, string>()
            {
                ["type"] = "service_account",
                ["project_id"] = "project",
                ["private_key_id"] = "key",
                ["private_key"] = rsa.ExportPkcs8PrivateKeyPem(),
                ["client_email"] = "sink@project.iam.gserviceaccount.com",
                ["client_id"] = "1",
                ["token_uri"] = "https://oauth2.googleapis.com/token"
            });
            return Files.Of.GoogleCloudStorage("bucket", credential);
        }

        public static string TempDirectory(string name)
        {
            var directory = Path.Combine(Path.GetTempPath(), "flowtide-cleanup-tests", name + "-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            return directory;
        }
    }

    /// <summary>
    /// A wrapper the user configured; classification never looks through it.
    /// </summary>
    internal class ForwardingStorage : IFileStorage
    {
        public ForwardingStorage(IFileStorage inner)
        {
            Inner = inner;
        }

        public IFileStorage Inner { get; }

        public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => Inner.OpenWrite(path, cancellationToken);

        public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default) => Inner.OpenRead(path, cancellationToken);

        public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => Inner.Ren(name, newName, cancellationToken);

        public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => Inner.ReadText(path, encoding, cancellationToken);

        public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => Inner.WriteText(path, contents, encoding, cancellationToken);

        public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default) => Inner.Ls(path, recurse, cancellationToken);

        public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => Inner.ReadAsJson<T>(path, cancellationToken);

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => Inner.WriteAsJson(path, value, writeIndented, cancellationToken);

        public Task Rm(IOPath path, CancellationToken cancellationToken = default) => Inner.Rm(path, cancellationToken);

        public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => Inner.Exists(path, cancellationToken);

        public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => Inner.Stat(path, cancellationToken);

        public void Dispose() => Inner.Dispose();
    }

    /// <summary>
    /// A wrapper the connector would own, so classification looks through it.
    /// </summary>
    internal sealed class OwnedForwardingStorage : ForwardingStorage, IConnectorOwnedStorage
    {
        public OwnedForwardingStorage(IFileStorage inner) : base(inner)
        {
        }
    }

    /// <summary>
    /// A connector-owned wrapper whose inner storage cannot be read.
    /// </summary>
    internal sealed class BrokenOwnedStorage : ForwardingStorage, IConnectorOwnedStorage
    {
        public BrokenOwnedStorage(IFileStorage inner) : base(inner)
        {
        }

        IFileStorage IConnectorOwnedStorage.Inner => throw new InvalidOperationException("Inner storage unavailable");
    }

    /// <summary>
    /// Objects in memory listed in ordinal order; a test rewrites listings and Stat answers and sees every request.
    /// </summary>
    internal sealed class ScriptedStorage : IFileStorage
    {
        private readonly SortedDictionary<string, byte[]> _objects = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        private readonly object _lock = new object();

        // Requests as "Verb path", in order
        public ConcurrentQueue<string> Requests { get; } = new ConcurrentQueue<string>();

        // Rewrites a listing before it is returned
        public Func<List<IOEntry>, List<IOEntry>>? AfterList { get; set; }

        // Runs after a listing is built and before it is returned
        public Action? OnListed { get; set; }

        // Replaces a Stat answer; it may throw
        public Func<IOPath, IOEntry?, IOEntry?>? StatAnswer { get; set; }

        public void Put(string full, int size = 1)
        {
            lock (_lock)
            {
                _objects[new IOPath(full).Full] = new byte[size];
            }
        }

        public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            var folder = (path ?? IOPath.Root).WTS;
            Requests.Enqueue($"Ls {folder}");
            var entries = new List<IOEntry>();
            lock (_lock)
            {
                foreach (var (name, content) in _objects)
                {
                    if (name.StartsWith(folder, StringComparison.Ordinal) && (recurse || name.IndexOf('/', folder.Length) < 0))
                    {
                        entries.Add(new IOEntry(name) { Size = content.Length });
                    }
                }
            }
            if (AfterList != null)
            {
                entries = AfterList(entries);
            }
            OnListed?.Invoke();
            return Task.FromResult<IReadOnlyCollection<IOEntry>>(entries);
        }

        public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
        {
            Requests.Enqueue($"Stat {path.Full}");
            IOEntry? entry;
            lock (_lock)
            {
                entry = _objects.TryGetValue(path.Full, out var content) ? new IOEntry(path.Full) { Size = content.Length } : null;
            }
            return Task.FromResult(StatAnswer != null ? StatAnswer(path, entry) : entry);
        }

        public Task Rm(IOPath path, CancellationToken cancellationToken = default)
        {
            Requests.Enqueue($"Rm {path.Full}");
            lock (_lock)
            {
                _objects.Remove(path.Full);
            }
            return Task.CompletedTask;
        }

        public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public void Dispose()
        {
        }
    }
}
