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
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class StoreListingQualificationTests
    {
        private static readonly string[] TableObjects =
        {
            "tables/t/_delta_log/00000000000000000000.json",
            "tables/t/_delta_log/.00000000000000000001.json.abc.tmp",
            "tables/t/_change_data/cdc-1.parquet",
            "tables/t/.flowtide-orphan-sweep",
            "tables/t/part-1.parquet",
            "tables/t/part-1.parquet.x",
            "tables/t/sub/deep/x.bin"
        };

        // The query value a service reads, form-decoded
        private static string? QueryValue(string requestLine, string key)
        {
            var target = requestLine.Split(' ')[1];
            var start = target.IndexOf('?');
            if (start < 0)
            {
                return null;
            }
            foreach (var part in target[(start + 1)..].Split('&'))
            {
                var eq = part.IndexOf('=');
                if (eq > 0 && part[..eq] == key)
                {
                    return HttpUtility.UrlDecode(part[(eq + 1)..]);
                }
            }
            return null;
        }

        private static string RawQueryValue(string requestLine, string key)
        {
            var target = requestLine.Split(' ')[1];
            var at = target.IndexOf(key + "=", StringComparison.Ordinal);
            var end = target.IndexOf('&', at);
            return end < 0 ? target[(at + key.Length + 1)..] : target[(at + key.Length + 1)..end];
        }

        private static List<string> Lists(LoopbackObjectStore server) => server.Requests.Where(x => x.Contains("comp=list") || x.Contains("list-type=2")).ToList();

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        [InlineData(StoreFlavor.S3)]
        public async Task PinnedRecursiveDeliveryUnderNonRootPrefix(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            foreach (var name in TableObjects)
            {
                server.Put(name);
            }

            var delivered = (await storage.Ls("/c/tables/t/", recurse: true)).Select(x => (x.Path.Full, x.Path.IsFolder, x.Size.HasValue)).ToList();

            // The client puts the immediate parent folders first, relative to the listed root and then under the container, so outside the folder
            var synthetic = flavor == StoreFlavor.AzureHns
                ? new[] { "/c/_change_data/", "/c/_delta_log/", "/c/sub/", "/c/sub/deep/" }
                : new[] { "/c/_change_data/", "/c/_delta_log/", "/c/sub/deep/" };
            var provider = flavor == StoreFlavor.AzureHns
                // A hierarchical account lists its directories as entries, the client reads them as empty files
                ? new[]
                {
                    "/c/tables/t/.flowtide-orphan-sweep", "/c/tables/t/_change_data", "/c/tables/t/_change_data/cdc-1.parquet", "/c/tables/t/_delta_log",
                    "/c/tables/t/_delta_log/.00000000000000000001.json.abc.tmp", "/c/tables/t/_delta_log/00000000000000000000.json", "/c/tables/t/part-1.parquet",
                    "/c/tables/t/part-1.parquet.x", "/c/tables/t/sub", "/c/tables/t/sub/deep", "/c/tables/t/sub/deep/x.bin"
                }
                : TableObjects.Select(x => "/c/" + x).OrderBy(x => x, StringComparer.Ordinal).ToArray();
            Assert.Equal(synthetic.Select(x => (x, true, false)).Concat(provider.Select(x => (x, false, true))), delivered);
            if (flavor == StoreFlavor.AzureHns)
            {
                var entries = await storage.Ls("/c/tables/t/", recurse: true);
                Assert.Equal(0, entries.Single(x => x.Path.Full == "/c/tables/t/_delta_log").Size);
            }
        }

        // Data where a final sort, a per-page insertion or sorted parents would each change the delivered sequence
        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        [InlineData(StoreFlavor.S3)]
        public async Task PinnedRecursiveDeliveryIsUnsortedFirstOccurrenceAfterPaging(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            foreach (var name in new[] { "A/t/_delta_log/00000000000000000000.json", "A/t/part-1.parquet", "A/t/part-1/x", "A/t/sub/deep/x.bin", "A/t/sub/y.bin" })
            {
                server.Put(name);
            }
            server.PageSize = _ => 2;

            var delivered = (await storage.Ls("/c/A/t/", recurse: true)).Select(x => (x.Path.Full, x.Path.IsFolder, x.Size.HasValue)).ToList();

            var synthetic = flavor == StoreFlavor.AzureHns
                ? new[] { "/c/_delta_log/", "/c/part-1/", "/c/sub/", "/c/sub/deep/" }
                : new[] { "/c/_delta_log/", "/c/part-1/", "/c/sub/deep/", "/c/sub/" };
            var provider = flavor == StoreFlavor.AzureHns
                ? new[] { "/c/A/t/_delta_log", "/c/A/t/_delta_log/00000000000000000000.json", "/c/A/t/part-1", "/c/A/t/part-1/x", "/c/A/t/part-1.parquet", "/c/A/t/sub", "/c/A/t/sub/deep", "/c/A/t/sub/deep/x.bin", "/c/A/t/sub/y.bin" }
                : new[] { "/c/A/t/_delta_log/00000000000000000000.json", "/c/A/t/part-1.parquet", "/c/A/t/part-1/x", "/c/A/t/sub/deep/x.bin", "/c/A/t/sub/y.bin" };
            Assert.Equal(synthetic.Select(x => (x, true, false)).Concat(provider.Select(x => (x, false, true))), delivered);
            Assert.Equal((provider.Length + 1) / 2, Lists(server).Count);
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task NonrecursiveListingKeepsProviderGroups(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            foreach (var name in new[] { "t/_delta_log/a.json", "t/_delta_log/b/x.json", "t/_delta_log/c.json" })
            {
                server.Put(name);
            }

            var delivered = (await storage.Ls("/c/t/_delta_log/")).Select(x => x.Path.Full).ToList();

            // Folders and files arrive in groups, not one sorted sequence: S3 files then prefixes, Azure each page's prefixes then its blobs
            var expected = flavor == StoreFlavor.S3
                ? new[] { "/c/t/_delta_log/a.json", "/c/t/_delta_log/c.json", "/c/t/_delta_log/b/" }
                : new[] { "/c/t/_delta_log/b/", "/c/t/_delta_log/a.json", "/c/t/_delta_log/c.json" };
            Assert.Equal(expected, delivered);
        }

        [Fact]
        public async Task AzureQualifiedMultiPageListingIncludesShortAndEmptyPages()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var storage = server.CreateStorage();
            var names = Enumerable.Range(0, 12).Select(i => $"t/_delta_log/{i:D20}.json").ToList();
            foreach (var name in names)
            {
                server.Put(name);
            }
            // A short page, an empty page that is not the last, then the rest
            server.PageSize = request => request switch { 1 => 2, 2 => 0, 3 => 5, _ => 5000 };

            var listed = (await storage.Ls("/c/t/_delta_log/")).Select(x => x.Path.Full).ToList();

            Assert.Equal(names.Select(x => "/c/" + x), listed);
            Assert.Equal(4, Lists(server).Count);
        }

        [Fact]
        public async Task AzureObservedPublicMarkerRoundTripsByteIdentically()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var storage = server.CreateStorage();
            foreach (var i in Enumerable.Range(0, 3))
            {
                server.Put($"a.b-c_d/T9/_delta_log/{i:D20}.json");
            }
            server.PageSize = request => 1;

            await storage.Ls("/c/a.b-c_d/T9/_delta_log/");

            var lists = Lists(server);
            Assert.Equal(3, lists.Count);
            for (int i = 1; i < lists.Count; i++)
            {
                var expected = LoopbackObjectStore.Marker($"a.b-c_d/T9/_delta_log/{i:D20}.json");
                // Unencoded on the wire, and still the exact marker: the format has no reserved characters for qualified names
                Assert.Equal(expected, RawQueryValue(lists[i], "marker"));
                Assert.DoesNotContain('+', expected);
            }
        }

        [Fact]
        public Task AzurePageErrorNeverReturnsShortenedList() => PageErrorNeverReturnsShortenedList(StoreFlavor.Azure);

        [Fact]
        public Task S3PageErrorNeverReturnsShortenedList() => PageErrorNeverReturnsShortenedList(StoreFlavor.S3);

        // A page failing, the first or a later one, with any status and on every retry, fails the whole listing
        private static async Task PageErrorNeverReturnsShortenedList(StoreFlavor flavor)
        {
            foreach (var failFrom in new[] { 1, 2, 3 })
            {
                foreach (var status in new[] { 400, 403, 404, 500, 503 })
                {
                    await using var server = new LoopbackObjectStore(flavor);
                    using var storage = server.CreateStorage();
                    foreach (var i in Enumerable.Range(0, 6))
                    {
                        server.Put($"t/_delta_log/{i:D20}.json");
                    }
                    server.PageSize = _ => 2;
                    var lists = 0;
                    server.Intercept = (method, target) => (target.Contains("comp=list") || target.Contains("list-type=2")) && Interlocked.Increment(ref lists) >= failFrom
                        ? (status, "<Error><Code>Injected</Code></Error>")
                        : null;

                    await Assert.ThrowsAnyAsync<Exception>(() => storage.Ls("/c/t/_delta_log/"));
                    // The failure came from the injected page; a later page carried a marker or token
                    var requests = Lists(server);
                    Assert.True(requests.Count >= failFrom, $"{flavor} {status}: page {failFrom} was never requested");
                    if (failFrom > 1)
                    {
                        Assert.True(requests[failFrom - 1].Contains("marker=") || requests[failFrom - 1].Contains("continuation-token="), requests[failFrom - 1]);
                    }
                }
            }
        }

        [Fact]
        public Task AzureStatDistinguishesAbsentFromHttpError() => StatDistinguishesAbsentFromHttpError(StoreFlavor.Azure);

        [Fact]
        public Task S3StatDistinguishesAbsentFromHttpError() => StatDistinguishesAbsentFromHttpError(StoreFlavor.S3);

        private static async Task StatDistinguishesAbsentFromHttpError(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            server.Put("t/_delta_log/00000000000000000000.json");

            Assert.Null(await storage.Stat("/c/t/_delta_log/00000000000000000001.json"));
            Assert.Equal(server.Names[0].Length, (await storage.Stat("/c/t/_delta_log/00000000000000000000.json"))!.Size);
            foreach (var status in new[] { 400, 403, 500, 503 })
            {
                var heads = 0;
                server.Intercept = (method, target) =>
                {
                    if (method != "HEAD")
                    {
                        return null;
                    }
                    Interlocked.Increment(ref heads);
                    return (status, "");
                };
                await Assert.ThrowsAnyAsync<Exception>(() => storage.Stat("/c/t/_delta_log/00000000000000000001.json"));
                Assert.True(heads > 0, $"{flavor} {status}: no HEAD reached the service");
            }
        }

        [Theory]
        [InlineData("tables/a.b-c_d/T9")]
        [InlineData("x")]
        public Task S3SafePrefixesRoundTripSemantically(string table)
        {
            return QualifiedPrefixesRoundTrip(StoreFlavor.S3, table);
        }

        [Theory]
        [InlineData("tables/a.b-c_d/T9")]
        [InlineData("x")]
        public Task AzureQualifiedPrefixesRoundTripSemantically(string table)
        {
            return QualifiedPrefixesRoundTrip(StoreFlavor.Azure, table);
        }

        // Every name the contract qualifies reaches the service as itself, and a sibling sharing it as a prefix stays out
        private static async Task QualifiedPrefixesRoundTrip(StoreFlavor flavor, string table)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            var contract = CleanupStoreContract.Classify(storage, $"{LoopbackObjectStore.Container}/{table}", false);
            Assert.True(contract.Eligible, contract.PathProblem ?? contract.Store.Detail);
            Assert.True(contract.QualifiesPrefix($"{table}/_delta_log/"));
            server.Put($"{table}/_delta_log/00000000000000000000.json");
            server.Put($"{table}x/_delta_log/00000000000000000000.json");

            var listed = await storage.Ls($"/c/{table}/_delta_log/");

            Assert.Equal($"/c/{table}/_delta_log/00000000000000000000.json", Assert.Single(listed).Path.Full);
            var list = Assert.Single(Lists(server));
            Assert.Equal($"{table}/_delta_log/", QueryValue(list, "prefix"));
            // Sent as written, slashes and dots unencoded, with the slash delimiter
            Assert.Equal($"{table}/_delta_log/", RawQueryValue(list, "prefix"));
            Assert.Equal("/", RawQueryValue(list, "delimiter"));
            Assert.True(contract.TryQualifyTarget("_delta_log/00000000000000000000.json", out var target));
            Assert.Equal(Assert.Single(listed).Path.Full, target.Full);
        }

        [Fact]
        public async Task S3ContinuationTokensRoundTripSemantically()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            using var storage = server.CreateStorage();
            var names = Enumerable.Range(0, 7).Select(i => $"t/_delta_log/{i:D20}.json").ToList();
            foreach (var name in names)
            {
                server.Put(name);
            }
            server.PageSize = _ => 3;

            var listed = (await storage.Ls("/c/t/_delta_log/")).Select(x => x.Path.Full).ToList();

            // Tokens hold '+', '/', '=', '&' and '%'; the service reads them back exactly
            Assert.Equal(names.Select(x => "/c/" + x), listed);
            var lists = Lists(server);
            Assert.Equal(3, lists.Count);
            Assert.Equal(LoopbackObjectStore.TokenPrefix + "3", QueryValue(lists[1], "continuation-token"));
            Assert.Equal(LoopbackObjectStore.TokenPrefix + "6", QueryValue(lists[2], "continuation-token"));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task AzureQualifiedDeleteRoundTripsAndPreservesNeighbor(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            server.Put("t/part-1.parquet");
            server.Put("t/part-1.parquet.x");
            server.Put("t/part-1");

            await storage.Rm("/c/t/part-1.parquet");

            Assert.Single(server.Requests, x => x == "DELETE /c/t/part-1.parquet HTTP/1.1");
            Assert.Null(await storage.Stat("/c/t/part-1.parquet"));
            Assert.True(server.Contains("t/part-1.parquet.x"));
            Assert.True(server.Contains("t/part-1"));
        }

        [Fact]
        public async Task AzurePathPrefixedEndpointDeleteMissesTheObject()
        {
            // An emulator style endpoint whose URL carries the account as a path segment
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure, "devstoreaccount1/");
            using var storage = server.CreateStorage();
            server.Put("t/part-1.parquet");

            Assert.Single(await storage.Ls("/c/t/"));
            await storage.Rm("/c/t/part-1.parquet");

            // The delete drops the endpoint's path, so it reaches another account; its not-found is swallowed
            Assert.Single(server.Requests, x => x == "DELETE /c/t/part-1.parquet HTTP/1.1");
            Assert.True(server.Contains("t/part-1.parquet"));
            Assert.NotNull(await storage.Stat("/c/t/part-1.parquet"));
            // So the contract never qualifies such an endpoint, not even with the assertion
            var contract = CleanupStoreContract.Classify(storage, "c/t", true);
            Assert.True(contract.Store.KnownFailed);
            Assert.False(contract.Store.ExactDeletes);
        }

        // A non-root table prefix with a folder marker object, a nested folder, a sibling table and enough entries for several pages
        private static void PutInventory(LoopbackObjectStore server)
        {
            foreach (var name in TableObjects)
            {
                server.Put(name);
            }
            server.Put("tables/t/_delta_log/00000000000000000001.json");
            server.Put("tables/t/_change_data/cdc-2.parquet");
            server.Put("tables/t/empty/", Array.Empty<byte>());
            server.Put("tables/tx/_delta_log/00000000000000000000.json");
            server.Put("tables/u/part-1.parquet");
        }

        // The block the pinned client inserts for the inventory, in its first-occurrence order
        private static string[] InventoryBlock(StoreFlavor flavor)
        {
            return flavor == StoreFlavor.AzureHns
                ? new[] { "/c/_change_data/", "/c/_delta_log/", "/c/sub/", "/c/sub/deep/" }
                : new[] { "/c/_change_data/", "/c/_delta_log/", "/c/sub/deep/" };
        }

        private static CleanupStoreContract Contract(IFileStorage storage, string table = "c/tables/t") => CleanupStoreContract.Classify(storage, table, false);

        private static bool Increasing(IReadOnlyList<string> names, Func<string, string, int> compare)
        {
            for (int i = 1; i < names.Count; i++)
            {
                if (compare(names[i - 1], names[i]) >= 0)
                {
                    return false;
                }
            }
            return true;
        }

        private static int CompareBytes(string left, string right) => Encoding.UTF8.GetBytes(left).AsSpan().SequenceCompareTo(Encoding.UTF8.GetBytes(right));

        [Fact]
        public void FilesOfClassificationIsPinned()
        {
            // The loaded Stowage is the qualified build
            Assert.Equal(PinnedStowage.InformationalVersion, typeof(IFileStorage).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion);
            Assert.Same(typeof(IFileStorage).Assembly, PinnedStowage.Assembly);
            var directory = StoreFixtures.TempDirectory(nameof(FilesOfClassificationIsPinned));
            var memory = Files.Of.InternalMemory(nameof(FilesOfClassificationIsPinned));
            var cases = new (IFileStorage Storage, StoreKind Kind, bool Pinned, ListingDelivery Delivery, bool Eligible, bool EligibleAsserted)[]
            {
                (Files.Of.LocalDisk(directory), StoreKind.LocalDisk, true, ListingDelivery.UnpagedSnapshot, true, true),
                (StoreFixtures.S3(), StoreKind.S3, true, ListingDelivery.PinnedCloud, true, true),
                (Files.Of.Minio(new Uri("http://127.0.0.1:9"), "key", "secret", "us-east-1"), StoreKind.S3, true, ListingDelivery.PinnedCloud, true, true),
                (StoreFixtures.Azure(), StoreKind.Azure, true, ListingDelivery.PinnedCloud, true, true),
                (Files.Of.AzureBlobStorage("account", Convert.ToBase64String(new byte[32])), StoreKind.Azure, true, ListingDelivery.PinnedCloud, true, true),
                // Deletes drop the endpoint's path segment
                (Files.Of.AzureBlobStorageWithLocalEmulator(), StoreKind.Azure, true, ListingDelivery.PinnedCloud, false, false),
                (StoreFixtures.Azure("devstoreaccount1/"), StoreKind.Azure, true, ListingDelivery.PinnedCloud, false, false),
                (StoreFixtures.Gcs(), StoreKind.Gcs, true, ListingDelivery.Ordered, false, false),
                (memory, StoreKind.InMemory, true, ListingDelivery.UnpagedSnapshot, false, true),
                // Another implementation of the pinned build is not qualified
                ((IFileStorage)Files.Of.VFS(), StoreKind.Unknown, true, ListingDelivery.Ordered, false, true),
                // A marker interface on a wrapper does not make it local disk
                (HookFileStorage.Wrap(Files.Of.LocalDisk(directory)), StoreKind.Unknown, false, ListingDelivery.Ordered, false, true),
                (new HookFileStorage(memory), StoreKind.Unknown, false, ListingDelivery.Ordered, false, true),
                // Connector-owned wrappers are looked through
                (new OwnedForwardingStorage(new OwnedForwardingStorage(StoreFixtures.S3())), StoreKind.S3, true, ListingDelivery.PinnedCloud, true, true),
                (new OwnedForwardingStorage(StoreFixtures.Gcs()), StoreKind.Gcs, true, ListingDelivery.Ordered, false, false)
            };
            foreach (var (storage, kind, pinned, delivery, eligible, eligibleAsserted) in cases)
            {
                var contract = CleanupStoreContract.Classify(storage, "c/t", false);
                Assert.Equal(kind, contract.Store.Kind);
                Assert.Equal(pinned, contract.Store.Pinned);
                Assert.Equal(delivery, contract.Store.Delivery);
                Assert.Equal(eligible, contract.Eligible);
                Assert.Equal(eligibleAsserted, CleanupStoreContract.Classify(storage, "c/t", true).Eligible);
            }
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        [InlineData(StoreFlavor.S3)]
        public async Task RecursiveImplicitFolderBlockIsRecomputedAndExcluded(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            PutInventory(server);
            var block = InventoryBlock(flavor);

            var delivered = (await storage.Ls("/c/tables/t/", recurse: true)).ToList();

            // The client's block comes first, outside the folder and without sizes
            Assert.Equal(block, delivered.Take(block.Length).Select(x => x.Path.Full));
            Assert.All(delivered.Take(block.Length), x => Assert.Null(x.Size));
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(Contract(storage), "/c/tables/t/", true, delivered.ToList(), out var listing));
            // Exactly that block is gone and every provider entry stays in delivered order
            Assert.Equal(delivered.Skip(block.Length).Select(x => x.Path.Full), listing!.Entries.Select(x => x.Path.Full));
            Assert.All(listing.Entries, x => Assert.NotNull(x.Size));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        [InlineData(StoreFlavor.S3)]
        public async Task TamperedOrMisplacedImplicitFolderBlockDeclines(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            PutInventory(server);
            var contract = Contract(storage);
            var delivered = (await storage.Ls("/c/tables/t/", recurse: true)).ToList();
            var block = InventoryBlock(flavor).Length;
            var provider = delivered.Skip(block).ToList();
            ListingCheck Check(IEnumerable<IOEntry> entries) => ListingEvidence.Check(contract, "/c/tables/t/", true, entries.ToList(), out _);
            List<IOEntry> Swap(int left, int right)
            {
                var list = delivered.ToList();
                (list[left], list[right]) = (list[right], list[left]);
                return list;
            }
            // A fresh entry of the same name, so no alteration reaches the delivered one
            IOEntry Altered(Action<IOEntry> alter)
            {
                var entry = new IOEntry(delivered[0].Path.Full);
                alter(entry);
                return entry;
            }
            var sized = Altered(x => x.Size = 0);
            Assert.Equal(ListingCheck.Passed, Check(delivered.Skip(1).Prepend(Altered(_ => { }))));
            var altered = new[]
            {
                Altered(x => x.MD5 = "md5"),
                Altered(x => x.LastModificationTime = DateTimeOffset.UnixEpoch),
                Altered(x => x.CreatedTime = DateTimeOffset.UnixEpoch),
                Altered(x => x.Properties["BlobType"] = "BlockBlob"),
                Altered(x => x.Metadata["k"] = "v"),
                Altered(x => x.Tag = "tag")
            };
            foreach (var entry in altered)
            {
                Assert.Null(entry.Size);
                Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Skip(1).Prepend(entry)));
            }

            Assert.Equal(ListingCheck.Passed, Check(delivered));
            // Missing, extra, reordered, renamed, reshaped or repeated synthetic folders
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Where((_, i) => i != 1)));
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Take(block).Append(new IOEntry("/c/zzz/")).Concat(provider)));
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(Swap(0, 1)));
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Skip(1).Prepend(new IOEntry("/c/_change_dat/"))));
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Skip(1).Prepend(sized)));
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Take(1).Concat(delivered.Take(block)).Concat(provider)));
            // Misplaced among or after the provider entries
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Skip(1).Take(block - 1).Concat(provider.Take(1)).Append(delivered[0]).Concat(provider.Skip(1))));
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(provider.Concat(delivered.Take(block))));
            Assert.Equal(ListingCheck.EnvelopeMismatch, Check(delivered.Append(new IOEntry("/c/tables/t/zzz/"))));

            // Provider regressions and duplicates are not envelope faults, and sorting never repairs them
            var first = delivered.FindIndex(x => x.Path.Full == "/c/tables/t/part-1.parquet");
            Assert.Equal(ListingCheck.OrderOrDuplicate, Check(Swap(first, first + 1)));
            Assert.Equal(ListingCheck.OrderOrDuplicate, Check(delivered.Take(first + 1).Append(delivered[first]).Concat(delivered.Skip(first + 1))));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        [InlineData(StoreFlavor.S3)]
        public async Task PinnedRecursiveInventoryPassesUnderNonRootPrefix(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            PutInventory(server);
            server.PageSize = _ => 3;

            var delivered = (await storage.Ls("/c/tables/t/", recurse: true)).ToList();
            var check = ListingEvidence.Check(Contract(storage), "/c/tables/t/", true, delivered, out var listing);

            Assert.True(Lists(server).Count > 2);
            Assert.Equal(ListingCheck.Passed, check);
            var names = Enumerable.Range(0, listing!.Count).Select(listing.RelativeName).ToList();
            Assert.Contains("_delta_log/00000000000000000000.json", names);
            Assert.Contains("_delta_log/00000000000000000001.json", names);
            Assert.Contains("_change_data/cdc-1.parquet", names);
            Assert.Contains(".flowtide-orphan-sweep", names);
            Assert.Contains("sub/deep/x.bin", names);
            if (flavor == StoreFlavor.S3)
            {
                // The pinned S3 client drops every slash-terminated key while parsing
                Assert.DoesNotContain("empty/", names);
            }
            else
            {
                // A genuine folder marker stays until candidates are filtered
                Assert.Equal(0, listing.Entries.Single(x => x.Path.Full == "/c/tables/t/empty/").Size);
            }
            Assert.DoesNotContain(names, x => x.StartsWith("x/") || x.Contains("tables/"));
            if (flavor == StoreFlavor.AzureHns)
            {
                // Directories of a hierarchical account stay as zero-size entries without a slash
                Assert.Equal(0, listing.Entries.Single(x => x.Path.Full == "/c/tables/t/_delta_log").Size);
                Assert.Contains("sub/deep", names);
            }
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task NonrecursiveEvidenceSubfoldersDeclineAcrossPages(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            for (int i = 0; i < 6; i++)
            {
                server.Put($"t/_delta_log/{i:D20}.json");
            }
            // Subfolders sorting between commits, so groups mix within pages
            server.Put("t/_delta_log/00000000000000000001.d/x.json");
            server.Put("t/_delta_log/_commits/y.json");
            server.PageSize = _ => 2;
            var contract = Contract(storage, "c/t");

            var delivered = (await storage.Ls("/c/t/_delta_log/")).ToList();

            Assert.True(Lists(server).Count > 2);
            Assert.Contains(delivered, x => x.Path.IsFolder);
            Assert.Equal(ListingCheck.UnsupportedSubfolder, ListingEvidence.Check(contract, "/c/t/_delta_log/", false, delivered, out _));

            // The same pages without subfolders pass
            await storage.Rm("/c/t/_delta_log/00000000000000000001.d/x.json");
            await storage.Rm("/c/t/_delta_log/_commits/y.json");
            var files = (await storage.Ls("/c/t/_delta_log/")).ToList();
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(contract, "/c/t/_delta_log/", false, files, out var listing));
            Assert.Equal(6, listing!.Count);
        }

        [Fact]
        public async Task S3Utf8OrderHandlesSupplementaryForeignNames()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            using var storage = server.CreateStorage();
            foreach (var name in new[] { "00000000000000000000.json", "a！", "a\U0001F600", "aé", "b" })
            {
                server.Put("t/_delta_log/" + name);
            }
            var contract = Contract(storage, "c/t");

            var delivered = (await storage.Ls("/c/t/_delta_log/")).ToList();
            var names = delivered.Select(x => x.Path.Full).ToList();

            // UTF-8 byte order, which UTF-16 ordinal order contradicts for supplementary characters
            Assert.True(Increasing(names, CompareBytes));
            Assert.False(Increasing(names, string.CompareOrdinal));
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(contract, "/c/t/_delta_log/", false, delivered, out _));
            var ordinal = delivered.OrderBy(x => x.Path.Full, StringComparer.Ordinal).ToList();
            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(contract, "/c/t/_delta_log/", false, ordinal, out _));
            // Foreign names are kept, never targets
            Assert.False(contract.TryQualifyTarget("_delta_log/a\U0001F600", out _));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task HnsSlashLowestWholeSequenceAccepted(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            foreach (var name in new[] { "t/sub/x.json", "t/sub-x.json", "t/sub.y", "t/_delta_log/00000000000000000000.json" })
            {
                server.Put(name);
            }
            server.PageSize = _ => 2;
            var folder = "/c/t/";

            var delivered = (await storage.Ls(folder, recurse: true)).ToList();
            var provider = delivered.SkipWhile(x => x.Path.IsFolder && x.Size == null).Select(x => x.Path.Full).ToList();

            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(Contract(storage, "c/t"), folder, true, delivered.ToList(), out _));
            if (flavor == StoreFlavor.AzureHns)
            {
                // Only the slash-lowest order holds, for the whole sequence
                Assert.False(Increasing(provider, CompareBytes));
                Assert.True(Increasing(provider, (left, right) => string.CompareOrdinal(left.Replace('/', '\u0001'), right.Replace('/', '\u0001'))));
                // S3 has no such order, so the same delivery declines there
                var s3 = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);
                Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(s3, folder, true, delivered.ToList(), out _));
            }
            else
            {
                Assert.True(Increasing(provider, CompareBytes));
            }
        }

        // The markdown tables of a file: each a header and its data rows, cells trimmed
        private static List<(string[] Header, List<string[]> Rows)> Tables(string text)
        {
            var tables = new List<(string[] Header, List<string[]> Rows)>();
            List<string[]>? current = null;
            foreach (var line in text.Split('\n').Select(x => x.Trim()))
            {
                if (!line.StartsWith('|'))
                {
                    current = null;
                    continue;
                }
                var cells = line.Trim('|').Split('|').Select(x => x.Trim()).ToArray();
                if (current == null)
                {
                    current = new List<string[]>();
                    tables.Add((cells, current));
                }
                else if (!cells.All(x => x.Trim('-', ':').Length == 0))
                {
                    current.Add(cells);
                }
            }
            return tables;
        }

        [Fact]
        public void EveryEligibleStoreHasAnEvidenceRow()
        {
            var tables = Tables(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "listing-evidence.md")));
            var (header, rows) = tables.Single(x => x.Header.Contains("Implementation type"));
            var implementation = Array.IndexOf(header, "Implementation type");
            var eligibility = Array.IndexOf(header, "Cleanup eligibility");
            var evidence = Array.IndexOf(header, "Evidence");
            var directory = StoreFixtures.TempDirectory(nameof(EveryEligibleStoreHasAnEvidenceRow));
            var representatives = new IFileStorage[]
            {
                Files.Of.LocalDisk(directory), StoreFixtures.S3(), StoreFixtures.Azure(), StoreFixtures.Azure("devstoreaccount1/"), StoreFixtures.Gcs(),
                Files.Of.InternalMemory(nameof(EveryEligibleStoreHasAnEvidenceRow)), new ForwardingStorage(StoreFixtures.S3())
            };
            foreach (var storage in representatives)
            {
                var store = CleanupStoreContract.Classify(storage, "c/t", false).Store;
                var row = store.Kind == StoreKind.Unknown
                    ? rows.Single(x => x[0].StartsWith("Any other"))
                    : rows.Single(x => x[implementation].Contains($"`{storage.GetType().FullName}`") && x[implementation].Contains("endpoint path") == (store.Kind == StoreKind.Azure && store.KnownFailed));
                var expected = store.KnownFailed ? "Ineligible" : store.NeedsAssertion ? "Asserted only" : "Eligible";
                Assert.StartsWith(expected, row[eligibility]);
                if (!store.KnownFailed)
                {
                    Assert.Matches("`[A-Z][A-Za-z0-9]+`", row[evidence]);
                }
            }

            // Every test an evidence column names exists
            var tests = typeof(StoreListingQualificationTests).Assembly.GetTypes()
                .SelectMany(x => x.GetMethods())
                .Where(x => x.GetCustomAttributes<FactAttribute>().Any())
                .Select(x => x.Name)
                .ToHashSet();
            var cited = 0;
            foreach (var (tableHeader, tableRows) in tables)
            {
                var column = Array.IndexOf(tableHeader, "Evidence");
                if (column < 0)
                {
                    continue;
                }
                foreach (var row in tableRows)
                {
                    foreach (Match match in Regex.Matches(row[column], "`([A-Z][A-Za-z0-9]+)`"))
                    {
                        Assert.Contains(match.Groups[1].Value, tests);
                        cited++;
                    }
                }
            }
            Assert.True(cited > 20, $"{cited} tests cited");
        }
    }
}
