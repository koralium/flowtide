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
using Stowage.Impl;
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

        [Fact]
        public async Task ConcurrentListAndWriteDoNotThrow()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{nameof(ConcurrentListAndWriteDoNotThrow)}"));
            for (int i = 0; i < 200; i++)
            {
                var name = $"/t/_delta_log/{i:D20}.json";
                var write = Task.Run(async () =>
                {
                    await using var stream = await storage.OpenWrite(name);
                    await stream.WriteAsync(new byte[1024]);
                });
                var list = Task.Run(() => storage.Ls("/t/_delta_log/"));
                await Task.WhenAll(write, list);
            }
        }

        [Fact]
        public async Task WrappersOverDifferentStoresDoNotSerialize()
        {
            var first = Files.Of.InternalMemory($"./{nameof(WrappersOverDifferentStoresDoNotSerialize)}_1");
            var second = Files.Of.InternalMemory($"./{nameof(WrappersOverDifferentStoresDoNotSerialize)}_2");
            await first.WriteText("/a.json", "1");
            await second.WriteText("/b.json", "2");
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var held = new HookFileStorage(new GatedReadStorage(first, entered, gate.Task));
            var other = new HookFileStorage(second);

            var blocked = held.ReadText("/a.json");
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            try
            {
                // A read held on one store must not stall a read on another
                Assert.Equal("2", await other.ReadText("/b.json").WaitAsync(TimeSpan.FromSeconds(10)));
            }
            finally
            {
                gate.TrySetResult();
            }
            Assert.Equal("1", await blocked);
        }

        [Fact]
        public void LocalDiskInterfaceIsForwarded()
        {
            var dir = Path.Combine(Path.GetTempPath(), $"hook-local-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            try
            {
                var inner = Files.Of.LocalDisk(dir);
                var local = Assert.IsAssignableFrom<ILocalDiskFileStorage>(HookFileStorage.Wrap(inner));
                Assert.Equal(((ILocalDiskFileStorage)inner).ToNativeLocalPath("/t/a.json"), local.ToNativeLocalPath("/t/a.json"));
                Assert.False(HookFileStorage.Wrap(Files.Of.InternalMemory($"./{nameof(LocalDiskInterfaceIsForwarded)}")) is ILocalDiskFileStorage);
            }
            finally
            {
                Directory.Delete(dir, true);
            }
        }

        [Fact]
        public async Task StampedIdentityChangesOnEveryWrite()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{nameof(StampedIdentityChangesOnEveryWrite)}")) { StampIdentity = true };
            await storage.WriteText("/t/a.json", "same");
            var first = Assert.Single(await storage.Ls("/t/")).LastModificationTime;
            await storage.WriteText("/t/a.json", "same");
            var second = Assert.Single(await storage.Ls("/t/")).LastModificationTime;

            Assert.NotNull(first);
            Assert.NotEqual(first, second);
            Assert.Equal(second, (await storage.Stat("/t/a.json"))!.LastModificationTime);
        }

        [Fact]
        public void InternalMemorySameIdIsOneInstance()
        {
            var id = $"./{nameof(InternalMemorySameIdIsOneInstance)}";
            Assert.Same(Files.Of.InternalMemory(id), Files.Of.InternalMemory(id));
        }

        private static async Task<string> ReadToEnd(Stream stream)
        {
            using var reader = new StreamReader(stream);
            return await reader.ReadToEndAsync();
        }

        [Fact]
        public async Task StampedIdentityChangesWhenReplacementBytesBecomeVisible()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{nameof(StampedIdentityChangesWhenReplacementBytesBecomeVisible)}")) { StampIdentity = true };
            await storage.WriteText("/t/a.json", "old");
            var replacement = await storage.OpenWrite("/t/a.json");
            await replacement.WriteAsync(Encoding.UTF8.GetBytes("new"));
            var beforePublication = Assert.Single(await storage.Ls("/t/")).LastModificationTime;
            Assert.Equal("old", await storage.ReadText("/t/a.json"));

            await replacement.DisposeAsync();
            Assert.Equal("new", await storage.ReadText("/t/a.json"));
            var afterPublication = Assert.Single(await storage.Ls("/t/")).LastModificationTime;
            Assert.NotEqual(beforePublication, afterPublication);
        }

        // Holds every read until the gate opens
        private sealed class GatedReadStorage : IFileStorage
        {
            private readonly IFileStorage _inner;
            private readonly TaskCompletionSource _entered;
            private readonly Task _gate;

            public GatedReadStorage(IFileStorage inner, TaskCompletionSource entered, Task gate)
            {
                _inner = inner;
                _entered = entered;
                _gate = gate;
            }

            public async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
            {
                _entered.TrySetResult();
                await _gate;
                return await _inner.OpenRead(path, cancellationToken);
            }

            public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => _inner.OpenWrite(path, cancellationToken);

            public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => _inner.Ren(name, newName, cancellationToken);

            public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.ReadText(path, encoding, cancellationToken);

            public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.WriteText(path, contents, encoding, cancellationToken);

            public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default) => _inner.Ls(path, recurse, cancellationToken);

            public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => _inner.ReadAsJson<T>(path, cancellationToken);

            public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => _inner.WriteAsJson(path, value, writeIndented, cancellationToken);

            public Task Rm(IOPath path, CancellationToken cancellationToken = default) => _inner.Rm(path, cancellationToken);

            public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => _inner.Exists(path, cancellationToken);

            public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => _inner.Stat(path, cancellationToken);

            public void Dispose() => _inner.Dispose();
        }

        // A test removes the file while a stream reads it
        [Fact]
        public async Task DeferredWriteLandsAfterRelease()
        {
            var memory = Files.Of.InternalMemory($"./{nameof(DeferredWriteLandsAfterRelease)}");
            var storage = new HookFileStorage(memory);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.DeferPublication = path => path.Full.EndsWith("late.json") ? release.Task : null;
            storage.PublicationParked = _ => parked.TrySetResult();

            var write = Task.Run(async () =>
            {
                await using var stream = await storage.OpenWrite("/t/late.json");
                await stream.WriteAsync(Encoding.UTF8.GetBytes("late"));
            });
            // The late write is written and waits at its publication
            await parked.Task.WaitAsync(TimeSpan.FromMinutes(1));
            await using (var other = await storage.OpenWrite("/t/other.json"))
            {
                await other.WriteAsync(Encoding.UTF8.GetBytes("other"));
            }

            // The later write is visible, the deferred one is not until it is released
            Assert.True(await storage.Exists("/t/other.json"));
            Assert.False(write.IsCompleted);
            Assert.False(await memory.Exists("/t/late.json"));
            release.SetResult();
            await write.WaitAsync(TimeSpan.FromMinutes(1));
            Assert.Equal("late", await storage.ReadText("/t/late.json"));
        }

        [Fact]
        public async Task StampKeepsProperties()
        {
            var memory = Files.Of.InternalMemory($"./{nameof(StampKeepsProperties)}");
            await memory.WriteText("/t/a.json", "a");
            var storage = new HookFileStorage(new PropertiesStorage(memory)) { StampIdentity = true };

            var listed = Assert.Single(await storage.Ls("/t/"));
            var stat = await storage.Stat("/t/a.json");

            foreach (var entry in new[] { listed, stat! })
            {
                Assert.Equal("etag-1", entry.Properties["ETag"]);
                Assert.Equal("v", entry.Metadata["k"]);
                Assert.Equal("tag", entry.Tag);
                Assert.NotNull(entry.LastModificationTime);
            }
        }

        // Fills the properties a cloud store reports
        private sealed class PropertiesStorage : IFileStorage
        {
            private readonly IFileStorage _inner;

            public PropertiesStorage(IFileStorage inner)
            {
                _inner = inner;
            }

            private static IOEntry Fill(IOEntry entry)
            {
                entry.Properties["ETag"] = "etag-1";
                entry.Metadata["k"] = "v";
                entry.Tag = "tag";
                return entry;
            }

            public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default) => _inner.OpenRead(path, cancellationToken);

            public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => _inner.OpenWrite(path, cancellationToken);

            public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => _inner.Ren(name, newName, cancellationToken);

            public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.ReadText(path, encoding, cancellationToken);

            public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.WriteText(path, contents, encoding, cancellationToken);

            public async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
            {
                return (await _inner.Ls(path, recurse, cancellationToken)).Select(Fill).ToList();
            }

            public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => _inner.ReadAsJson<T>(path, cancellationToken);

            public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => _inner.WriteAsJson(path, value, writeIndented, cancellationToken);

            public Task Rm(IOPath path, CancellationToken cancellationToken = default) => _inner.Rm(path, cancellationToken);

            public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => _inner.Exists(path, cancellationToken);

            public async Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
            {
                var entry = await _inner.Stat(path, cancellationToken);
                return entry == null ? null : Fill(entry);
            }

            public void Dispose() => _inner.Dispose();
        }

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
