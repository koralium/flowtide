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
using System.Web;
using static FlowtideDotNet.Connector.DeltaLake.Tests.OrphanSweepTests;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // The loopback table for a pinned run
        internal OrphanTable PinnedTable(LoopbackObjectStore server) => LoopbackTable(server);

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task PinnedRecursiveInventoryPassesUnderNonRootPrefix(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var run = await PinnedOrphanRun.SeedAsync(this, server);
            run.PutOld("part-old.parquet", 3);
            run.PutOld("_change_data/cdc-1.parquet", 4);
            // Written at the sweep's time
            server.Put("tables/t/part-young.parquet", new byte[5]);
            // A sibling table whose name extends this one's
            server.Put("tables/tx/_delta_log/00000000000000000000.json", new byte[2], PinnedOrphanRun.Old);
            server.Put("tables/tx/part-old.parquet", new byte[3], PinnedOrphanRun.Old);

            var result = await run.SweepAsync();

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            // Old commits stay out by their log segment
            run.AssertFrozen(("_change_data/cdc-1.parquet", 4), ("part-old.parquet", 3));
            var counts = run.Counts!.Value;
            Assert.Equal(PinnedOrphanRun.Head + 1 + (flavor == StoreFlavor.AzureHns ? 1 : 0), counts.Log);
            Assert.Equal(1, counts.Reference);
            Assert.True(server.Contains("tables/t/.flowtide-orphan-sweep"));
            Assert.Equal(run.Delivered!.Count, counts.Listed);
            Assert.Equal(counts.Listed, run.Counters.OrphanListed);
            var requests = run.Requests();
            Assert.True(PinnedOrphanRun.Lists(requests).Count > 2);
            var recursive = run.RecursiveListing(requests);
            Assert.True(recursive.Count > 1);
            Assert.Equal(PinnedOrphanRun.Successor, requests[recursive[^1] + 1]);
            Assert.DoesNotContain(requests, x => x.StartsWith("DELETE ", StringComparison.Ordinal));
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task RecursiveImplicitFolderBlockIsRecomputedAndExcluded(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var run = await PinnedOrphanRun.SeedAsync(this, server);
            run.PutOld("_change_data/cdc-1.parquet", 4);
            run.PutOld("sub/deep/x.bin", 5);
            run.PutOld("q/part-q.parquet", 2);
            // A nested table whose log folder the client also names inside this table as q/_delta_log/
            run.PutOld("tables/t/q/_delta_log/00000000000000000000.json", 2);

            var result = await run.SweepAsync();

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            var delivered = run.Delivered!;
            var block = delivered.TakeWhile(x => x.Path.IsFolder && x.Size == null).Select(x => x.Path.Full).ToList();
            Assert.Contains("/c/tables/t/q/_delta_log/", block);
            var counts = run.Counts!.Value;
            Assert.Equal(delivered.Count, counts.Listed);
            Assert.Equal(delivered.Count - block.Count, counts.Provider);
            // The provider's tables/t/q only
            Assert.Equal(1, counts.NestedRoots);
            run.AssertFrozen(("_change_data/cdc-1.parquet", 4), ("q/part-q.parquet", 2), ("sub/deep/x.bin", 5));
        }

        [Theory]
        [InlineData(StoreFlavor.S3, "swapped")]
        [InlineData(StoreFlavor.S3, "missing")]
        [InlineData(StoreFlavor.S3, "misplaced")]
        [InlineData(StoreFlavor.Azure, "swapped")]
        [InlineData(StoreFlavor.Azure, "missing")]
        [InlineData(StoreFlavor.Azure, "misplaced")]
        [InlineData(StoreFlavor.AzureHns, "swapped")]
        [InlineData(StoreFlavor.AzureHns, "missing")]
        [InlineData(StoreFlavor.AzureHns, "misplaced")]
        public async Task TamperedOrMisplacedImplicitFolderBlockDeclines(StoreFlavor flavor, string tamper)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var run = await PinnedOrphanRun.SeedAsync(this, server);
            run.PutOld("_change_data/cdc-1.parquet", 4);
            run.PutOld("sub/deep/x.bin", 5);
            run.PutOld("part-old.parquet", 3);

            var result = await run.SweepAsync(listed =>
            {
                var block = listed.TakeWhile(x => x.Path.IsFolder && x.Size == null).Count();
                var provider = listed.Skip(block).ToList();
                switch (tamper)
                {
                    case "swapped":
                        (listed[0], listed[1]) = (listed[1], listed[0]);
                        return listed;
                    case "missing":
                        listed.RemoveAt(1);
                        return listed;
                    default:
                        // The first folder after the first provider entry
                        return listed.Skip(1).Take(block - 1).Concat(provider.Take(1)).Append(listed[0]).Concat(provider.Skip(1)).ToList();
                }
            });

            Assert.Equal(OutcomeKind.Declined, result.Kind);
            Assert.Equal(MaintenanceReason.ListingIneligible, result.Reason);
            Assert.Equal(1, run.Counters.ListingDeclines(ListingCheck.EnvelopeMismatch));
            Assert.True(run.Delivered!.TakeWhile(x => x.Path.IsFolder && x.Size == null).Count() >= 3);
            Assert.Null(run.Frozen);
            var requests = run.Requests();
            var recursive = run.RecursiveListing(requests);
            // Preparation's probe only
            Assert.Single(requests, x => x == PinnedOrphanRun.Successor);
            Assert.DoesNotContain(requests.Skip(recursive[^1] + 1), x => x.StartsWith("HEAD ", StringComparison.Ordinal));
            Assert.DoesNotContain(requests, x => x.StartsWith("DELETE ", StringComparison.Ordinal));
            Assert.Empty(run.Context.Scratch.Files);
            Assert.Equal(0, run.Context.Memory.Managed);
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task NestedTableSubtreeExcluded(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var run = await PinnedOrphanRun.SeedAsync(this, server);
            run.PutOld("nested/_delta_log/00000000000000000000.json", 2);
            run.PutOld("nested/part-old.parquet", 3);
            run.PutOld("nested/deeper/x.parquet", 4);
            run.PutOld("a-old.parquet", 5);

            var result = await run.SweepAsync();

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            run.AssertFrozen(("a-old.parquet", 5));
            var counts = run.Counts!.Value;
            Assert.Equal(1, counts.NestedRoots);
            // The two files, and on a hierarchical namespace the root's own directory and nested/deeper
            Assert.Equal(flavor == StoreFlavor.AzureHns ? 4 : 2, counts.Nested);
            Assert.Equal(0, counts.UnknownSize);
            Assert.Equal(0, run.Counters.OrphanKept(KeepReason.UnknownSize));
            var directory = run.Delivered!.SingleOrDefault(x => x.Path.Full == "/c/tables/t/nested/_delta_log");
            if (flavor == StoreFlavor.AzureHns)
            {
                // Also listed as a directory entry
                Assert.NotNull(directory);
                Assert.Equal(0, directory.Size);
                Assert.Equal(0, run.Delivered!.Single(x => x.Path.Full == "/c/tables/t/nested").Size);
            }
            else
            {
                // Found from its files alone
                Assert.Null(directory);
            }
        }
    }

    // A loopback table at c/tables/t with old commits 0 to 2, swept once at the kit's time over listing pages of three
    internal sealed class PinnedOrphanRun
    {
        public const long Head = 2;

        // Older than any cutoff a sweep at the kit's time computes
        public static readonly DateTimeOffset Old = Now - TimeSpan.FromDays(30);

        // The exact probe of the commit after the head
        public static readonly string Successor = $"HEAD /c/tables/t/_delta_log/{Head + 1:D20}.json HTTP/1.1";

        private PinnedOrphanRun(LoopbackObjectStore server, OrphanTable table, CleanupContext context)
        {
            Server = server;
            Table = table;
            Context = context;
        }

        public LoopbackObjectStore Server { get; }

        public OrphanTable Table { get; }

        public CleanupContext Context { get; }

        public RetentionCounters Counters { get; } = new RetentionCounters();

        // What the frozen hook saw, null when it never ran
        public List<FrozenCandidate>? Frozen { get; private set; }

        public OrphanCounts? Counts { get; private set; }

        // The recursive listing as the client delivered it
        public List<IOEntry>? Delivered { get; private set; }

        public static async Task<PinnedOrphanRun> SeedAsync(OrphanSweepTests kit, LoopbackObjectStore server)
        {
            server.Now = Old;
            var table = kit.PinnedTable(server);
            await RetainedTableAsync(table.Raw, table.Name, Head);
            server.Now = Now;
            server.PageSize = _ => 3;
            return new PinnedOrphanRun(server, table, kit.OrphanContext(Head));
        }

        // An old object below the table
        public void PutOld(string relative, int size) => Server.Put("tables/t/" + relative, new byte[size], Old);

        // One sweep, with the recursive listing rewritten below the meter when given
        public Task<CleanupResult> SweepAsync(Func<List<IOEntry>, IReadOnlyCollection<IOEntry>>? rewrite = null)
        {
            Table.Owned.AfterRecursiveLs = (_, listed) =>
            {
                Delivered = listed.ToList();
                return rewrite != null ? rewrite(listed.ToList()) : listed;
            };
            var sweep = Sweep(Table.Configuration, Table.Owned, counters: Counters);
            sweep.FrozenHookForTests = inventory =>
            {
                Frozen = OrphanSweepTests.Frozen(inventory, Table);
                Counts = inventory.Counts;
            };
            return sweep.RunAsync(Context);
        }

        // Exactly these old candidates, by name below the table
        public void AssertFrozen(params (string Relative, long Size)[] expected)
        {
            Assert.NotNull(Frozen);
            Assert.Equal(expected.OrderBy(x => x.Relative, StringComparer.Ordinal).Select(x => new FrozenCandidate(x.Relative, x.Size, Old)), Frozen.OrderBy(x => x.Relative, StringComparer.Ordinal));
        }

        public List<string> Requests() => Server.Requests.ToList();

        // Indexes of the listing requests
        public static List<int> Lists(List<string> requests) => Enumerable.Range(0, requests.Count).Where(i => requests[i].Contains("comp=list") || requests[i].Contains("list-type=2")).ToList();

        // Indexes of the pages without a delimiter, asserted to be one listing of the table's folder
        public List<int> RecursiveListing(List<string> requests)
        {
            var pages = Lists(requests).Where(i => string.IsNullOrEmpty(Query(requests[i], "delimiter"))).ToList();
            Assert.NotEmpty(pages);
            Assert.All(pages, i => Assert.Equal("tables/t/", Query(requests[i], "prefix")));
            Assert.Equal(Enumerable.Range(pages[0], pages.Count), pages);
            // Only the first page starts without a continuation
            Assert.Equal(pages.Take(1), pages.Where(i => string.IsNullOrEmpty(Query(requests[i], "marker") ?? Query(requests[i], "continuation-token"))));
            Assert.Single(Table.Owned.Recursive);
            return pages;
        }

        // A request line's query value as the service reads it
        private static string? Query(string requestLine, string key)
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
    }
}
