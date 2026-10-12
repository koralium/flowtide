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
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class HookFileStorageTests
    {
        [Theory]
        [InlineData("OpenRead")]
        [InlineData("ReadText")]
        [InlineData("ReadAsJson")]
        public async Task ARemoveDuringAReadIsNotUndone(string read)
        {
            var memory = Files.Of.InternalMemory($"./{nameof(ARemoveDuringAReadIsNotUndone)}_{read}");
            await memory.WriteText("/test/a.json", "\"foreign\"");
            var storage = new HookFileStorage(new RemoveDuringReadStorage(memory));

            var content = read switch
            {
                "OpenRead" => await ReadToEnd((await storage.OpenRead("/test/a.json"))!),
                "ReadText" => await storage.ReadText("/test/a.json"),
                _ => JsonSerializer.Serialize(await storage.ReadAsJson<string>("/test/a.json"))
            };

            Assert.Equal("\"foreign\"", content);
            Assert.False(await memory.Exists("/test/a.json"));
        }

        private static async Task<string> ReadToEnd(Stream stream)
        {
            using var reader = new StreamReader(stream);
            return await reader.ReadToEndAsync();
        }

        // A test removes the file while a stream reads it
        private sealed class RemoveDuringReadStorage : IFileStorage
        {
            private readonly IFileStorage _inner;

            public RemoveDuringReadStorage(IFileStorage inner)
            {
                _inner = inner;
            }

            public async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
            {
                var stream = await _inner.OpenRead(path, cancellationToken);
                await _inner.Rm(path, cancellationToken);
                return stream;
            }

            public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => _inner.OpenWrite(path, cancellationToken);

            public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => _inner.Ren(name, newName, cancellationToken);

            // Reads through OpenRead and disposes the stream, as Stowage does
            public async Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default)
            {
                using var stream = await OpenRead(path, cancellationToken);
                if (stream == null)
                {
                    return null;
                }
                using var reader = new StreamReader(stream, encoding ?? Encoding.UTF8);
                return await reader.ReadToEndAsync(cancellationToken);
            }

            public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.WriteText(path, contents, encoding, cancellationToken);

            public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default) => _inner.Ls(path, recurse, cancellationToken);

            public async Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default)
            {
                var json = await ReadText(path, null, cancellationToken);
                return json == null ? default : JsonSerializer.Deserialize<T>(json);
            }

            public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => _inner.WriteAsJson(path, value, writeIndented, cancellationToken);

            public Task Rm(IOPath path, CancellationToken cancellationToken = default) => _inner.Rm(path, cancellationToken);

            public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => _inner.Exists(path, cancellationToken);

            public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => _inner.Stat(path, cancellationToken);

            public void Dispose() => _inner.Dispose();
        }
    }
}
