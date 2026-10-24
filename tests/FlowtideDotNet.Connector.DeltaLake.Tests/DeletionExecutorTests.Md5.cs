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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeletionExecutorTests
    {
        private const string Md5TargetPath = "/c/tables/t/orphan.parquet";
        private const string FrozenMd5 = "0102030405060708090a0b0c0d0e0f10";
        private const string ChangedMd5 = "101112131415161718191a1b1c1d1e1f";
        private static readonly DateTimeOffset s_md5Modified = new DateTimeOffset(2009, 9, 9, 9, 20, 2, TimeSpan.Zero);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AzureOrphanSubmissionComparesTheListedMd5(bool changed)
        {
            await using var server = new AzureMd5Store(FrozenMd5, changed ? ChangedMd5 : FrozenMd5);
            using var storage = server.CreateStorage();
            var configuration = CleanupConfiguration.Validate(storage, "c/tables/t", false,
                new CleanupOptIns(OrphanSweepTests.Retention, OrphanSweepTests.Interval));
            var executor = new DeletionExecutor(configuration, storage);
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);

            // The pinned listing reports the digest in base64, as the sweep freezes it
            var listed = Assert.Single(await storage.Ls(new IOPath("/c/tables/t/"), recurse: true, server.Stop));
            Assert.Equal(Md5TargetPath, listed.Path.Full);
            Assert.Equal(Convert.ToBase64String(Convert.FromHexString(FrozenMd5)), listed.MD5);
            var memory = new JobMemory();
            var payload = new byte[OrphanCandidatePayload.MaxLength];
            var length = OrphanCandidatePayload.Encode(payload, listed.Size, listed.LastModificationTime!.Value, listed.MD5);
            using var writer = new OrphanTargetsWriter(scratch, memory);
            writer.Add(listed.Path.Full, payload.AsSpan(0, length));
            using var targets = writer.Finish();
            var read = new List<DeletionTarget>();
            await foreach (var target in targets.Source(executor, () => memory).ReadFrom(0, server.Stop))
            {
                read.Add(target);
            }
            var candidate = Assert.Single(read);
            Assert.Equal(KeepReason.None, executor.Check(candidate));
            Assert.Equal(3, candidate.Size);
            Assert.Equal(s_md5Modified, candidate.Modified);
            Assert.Equal(0, memory.Managed);

            // The pinned Stat reports the digest in hex while size and time stay unchanged
            var probed = await storage.Stat(Md5TargetPath, server.Stop);
            Assert.NotNull(probed);
            Assert.Equal(changed ? ChangedMd5 : FrozenMd5, probed.MD5);
            Assert.Equal(candidate.Size, probed.Size);
            Assert.Equal(candidate.Modified, probed.LastModificationTime);

            var outcome = await executor.DeleteAsync(candidate, orphan: true, server.Stop);

            Assert.Equal(2, server.Requests.Count(x => x == "HEAD " + Md5TargetPath + " HTTP/1.1"));
            if (changed)
            {
                Assert.Equal(DeleteStatus.Kept, outcome.Status);
                Assert.Equal(KeepReason.IdentityChanged, outcome.Reason);
                Assert.DoesNotContain(server.Requests, x => x.StartsWith("DELETE ", StringComparison.Ordinal));
                Assert.False(server.Deleted);
            }
            else
            {
                Assert.Equal(DeleteStatus.Deleted, outcome.Status);
                Assert.Equal("DELETE " + Md5TargetPath + " HTTP/1.1", Assert.Single(server.Requests.Where(x => x.StartsWith("DELETE ", StringComparison.Ordinal))));
                Assert.True(server.Deleted);
            }
        }

        [Fact]
        public async Task SliceBufferChargesEachTargetsFrozenMd5()
        {
            // Two targets fit the buffer by their keys alone, one with its frozen MD5
            var table = Scripted();
            table.Put("m0.parquet", "m1.parquet");
            var listed = Convert.ToBase64String(Convert.FromHexString(FrozenMd5));
            var keyCharge = DeletionRun.TargetOverheadBytes + 2L * "/c/t/m0.parquet".Length;
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), new[] { "m0.parquet", "m1.parquet" }.Select(x => table.Target(x) with { Md5 = listed }),
                new DeletionRunSettings() { MaxBufferBytes = 2 * keyCharge });
            var memory = new JobMemory();

            var first = await run.RunSliceAsync(Context(memory: memory));

            Assert.Equal(SliceStatus.Progressed, first.Status);
            Assert.Equal(1, first.Issued);
            Assert.True(memory.ManagedPeak >= keyCharge + DeletionRun.Md5OverheadBytes + 2L * listed.Length);
            Assert.Equal(0, memory.Managed);
            Assert.Equal(1, (await run.RunSliceAsync(Context())).Issued);
            Assert.Equal(2, run.Cursor);
        }

        // One blob at the target path, listed and probed with Content-MD5 through the real pinned client
        private sealed class AzureMd5Store : IAsyncDisposable
        {
            private readonly TcpListener _listener = new TcpListener(IPAddress.Loopback, 0);
            private readonly CancellationTokenSource _stop = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            private readonly string _listedMd5;
            private readonly string _probedMd5;
            private readonly Task _serving;

            public AzureMd5Store(string listedMd5, string probedMd5)
            {
                _listedMd5 = Convert.ToBase64String(Convert.FromHexString(listedMd5));
                _probedMd5 = Convert.ToBase64String(Convert.FromHexString(probedMd5));
                _listener.Start();
                _serving = ServeAsync();
            }

            public ConcurrentQueue<string> Requests { get; } = new ConcurrentQueue<string>();

            public bool Deleted { get; private set; }

            public CancellationToken Stop => _stop.Token;

            public IFileStorage CreateStorage() => Files.Of.AzureBlobStorage(
                new Uri($"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/"), "account", Convert.ToBase64String(new byte[32]));

            private async Task ServeAsync()
            {
                try
                {
                    while (true)
                    {
                        using var client = await _listener.AcceptTcpClientAsync(Stop);
                        await using var stream = client.GetStream();
                        using var reader = new StreamReader(stream, Encoding.ASCII, false, 1024, leaveOpen: true);
                        var request = await reader.ReadLineAsync(Stop) ?? throw new IOException("Missing request line");
                        Requests.Enqueue(request);
                        while (await reader.ReadLineAsync(Stop) is { Length: > 0 })
                        {
                        }
                        string response;
                        if (request.StartsWith("GET /c?", StringComparison.Ordinal) && request.Contains("comp=list", StringComparison.Ordinal))
                        {
                            var body = "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ContainerName=\"c\"><Prefix>tables/t/</Prefix><Blobs>"
                                + "<Blob><Name>tables/t/orphan.parquet</Name><Properties><Last-Modified>Wed, 09 Sep 2009 09:20:02 GMT</Last-Modified>"
                                + $"<Etag>0x8CBFF45D8A29A19</Etag><Content-Length>3</Content-Length><Content-Type>application/octet-stream</Content-Type><Content-MD5>{_listedMd5}</Content-MD5>"
                                + "<BlobType>BlockBlob</BlobType></Properties></Blob></Blobs><NextMarker /></EnumerationResults>";
                            response = $"HTTP/1.1 200 OK\r\nContent-Length: {Encoding.UTF8.GetByteCount(body)}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n" + body;
                        }
                        else if (request == "HEAD " + Md5TargetPath + " HTTP/1.1" && !Deleted)
                        {
                            response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Type: application/octet-stream\r\n"
                                + $"Content-MD5: {_probedMd5}\r\nLast-Modified: Wed, 09 Sep 2009 09:20:02 GMT\r\n"
                                + "x-ms-blob-type: BlockBlob\r\nETag: \"probe-etag\"\r\nConnection: close\r\n\r\n";
                        }
                        else if (request == "DELETE " + Md5TargetPath + " HTTP/1.1")
                        {
                            Deleted = true;
                            response = "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        }
                        else
                        {
                            response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        }
                        await stream.WriteAsync(Encoding.UTF8.GetBytes(response), Stop);
                        await stream.FlushAsync(Stop);
                    }
                }
                catch (Exception e) when (_stop.IsCancellationRequested && e is OperationCanceledException or ObjectDisposedException or SocketException)
                {
                }
            }

            public async ValueTask DisposeAsync()
            {
                _stop.Cancel();
                _listener.Stop();
                await _serving;
                _stop.Dispose();
            }
        }
    }
}
