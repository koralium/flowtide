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
using System.Web;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // The table's log folder as a listing request names it
        private const string LogPrefix = "tables/t/_delta_log/";

        // One listing page: without a delimiter, of the log folder, and resumed at a marker or token
        private readonly record struct ListingPage(bool Recursive, bool Log, bool Continued);

        // Rejects continuation pages with the fixture's own answer to a marker it cannot resume at
        private sealed class MarkerRejection
        {
            private readonly object _gate = new object();
            private readonly (int Status, string Body) _answer;
            private readonly Func<ListingPage, int, bool> _rejects;
            private bool _recursiveSeen;
            private int _logListings;
            private int _rejected;

            private MarkerRejection(StoreFlavor flavor, Func<ListingPage, int, bool> rejects)
            {
                _answer = flavor == StoreFlavor.S3 ? (400, "<Error><Code>InvalidArgument</Code></Error>") : (400, "<Error><Code>InvalidQueryParameterValue</Code></Error>");
                _rejects = rejects;
            }

            public int Rejected => Volatile.Read(ref _rejected);

            // The root listing's continuations, or those of the nth log listing after it
            public static MarkerRejection AtSweepListing(StoreFlavor flavor, int logListing) =>
                new MarkerRejection(flavor, (page, listing) => page.Recursive ? logListing == 0 : page.Log && logListing > 0 && listing == logListing);

            // Every log listing's continuations
            public static MarkerRejection AtEveryLogListing(StoreFlavor flavor) => new MarkerRejection(flavor, (page, _) => page.Log);

            public (int Status, string Body)? Answer(string method, string target)
            {
                if (method != "GET" || PageOf(target) is not { } page)
                {
                    return null;
                }
                lock (_gate)
                {
                    if (page.Recursive)
                    {
                        _recursiveSeen = true;
                    }
                    else if (page.Log && _recursiveSeen && !page.Continued)
                    {
                        _logListings++;
                    }
                    if (!page.Continued || !_rejects(page, _logListings))
                    {
                        return null;
                    }
                    _rejected++;
                    return _answer;
                }
            }
        }

        // A request target's listing page, null when it lists nothing
        private static ListingPage? PageOf(string target)
        {
            if (!target.Contains("comp=list") && !target.Contains("list-type=2"))
            {
                return null;
            }
            return new ListingPage(string.IsNullOrEmpty(QueryValue(target, "delimiter")), QueryValue(target, "prefix") == LogPrefix,
                !string.IsNullOrEmpty(QueryValue(target, "marker")) || !string.IsNullOrEmpty(QueryValue(target, "continuation-token")));
        }

        // A query value as the service decodes it
        private static string? QueryValue(string target, string key)
        {
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

        [Theory]
        [InlineData(StoreFlavor.S3, "root")]
        [InlineData(StoreFlavor.S3, "second")]
        [InlineData(StoreFlavor.S3, "third")]
        [InlineData(StoreFlavor.Azure, "root")]
        [InlineData(StoreFlavor.Azure, "second")]
        [InlineData(StoreFlavor.Azure, "third")]
        // The loopback serves the flat marker on hierarchical accounts too: rejection handling, not the HNS marker format
        [InlineData(StoreFlavor.AzureHns, "root")]
        [InlineData(StoreFlavor.AzureHns, "second")]
        [InlineData(StoreFlavor.AzureHns, "third")]
        public async Task RejectedMarkerDuringTheSweepDeclinesWithoutDeleting(StoreFlavor flavor, string listing)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var pinned = await PinnedOrphanRun.SeedAsync(this, server);
            var table = pinned.Table;
            pinned.PutOld("old-a.parquet", 3);
            // Every listing has continuation pages
            server.PageSize = _ => 1;
            var kit = await OrphanInvariants.CaptureAsync(table, server);
            var rejection = MarkerRejection.AtSweepListing(flavor, listing switch { "root" => 0, "second" => 1, _ => 2 });
            server.Intercept = rejection.Answer;
            var sweep = Sweep(table.Configuration, table.Owned, counters: pinned.Counters);
            var runs = Registered(sweep);

            var (result, seen) = await RunAsync(sweep, table, pinned.Context);

            Assert.True(rejection.Rejected > 0, $"No continuation page of the {listing} listing was requested");
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.Transient);
            Assert.Contains($"The orphan sweep's {(listing == "root" ? "root" : listing + " log")} listing failed", result.Detail);
            Assert.Empty(runs);
            Assert.Single(table.Owned.Recursive);
            var requests = server.Requests.ToList();
            Assert.DoesNotContain(requests, x => x.StartsWith("DELETE ", StringComparison.Ordinal));
            if (listing == "root")
            {
                Assert.Null(seen);
                var rejected = requests.FindIndex(x => PageOf(x.Split(' ')[1]) is { Recursive: true, Continued: true });
                Assert.True(rejected >= 0);
                Assert.DoesNotContain(requests.Skip(rejected), x => x == PinnedOrphanRun.Successor);
            }
            else
            {
                Assert.NotNull(seen);
            }
            await kit.AssertAsync(runs, 1, contexts: pinned.Context);

            // The next sweep, with every marker served
            server.Intercept = null;
            var again = OrphanContext(PinnedOrphanRun.Head);
            var (next, _) = await RunAsync(sweep, table, again);

            AssertResult(next, OutcomeKind.Completed, MaintenanceReason.None);
            var run = Assert.Single(runs);
            Assert.Equal(new[] { "old-a.parquet" }, kit.Record(run));
            Assert.Equal(2, table.Owned.Recursive.Count);
            await DeleteTurnsAsync(run, PinnedOrphanRun.Head);
            Assert.False(server.Contains("tables/t/old-a.parquet"));
            await kit.AssertAsync(runs, 2, contexts: new[] { pinned.Context, again });
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        // The loopback serves the flat marker on hierarchical accounts too: rejection handling, not the HNS marker format
        [InlineData(StoreFlavor.AzureHns)]
        public async Task RejectedMarkerAtADeleteTurnKeepsTheRun(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var pinned = await PinnedOrphanRun.SeedAsync(this, server);
            var table = pinned.Table;
            pinned.PutOld("old-a.parquet", 3);
            pinned.PutOld("old-b.parquet", 3);
            var kit = await OrphanInvariants.CaptureAsync(table, server);
            var ledger = new CapacityLedger(() => default);
            var key = OrphanRun.LeaseKeyOf(ProducerKey(table));
            var sweep = Sweep(table.Configuration, table.Owned, counters: pinned.Counters);
            var runs = Registered(sweep);
            var context = OrphanContext(PinnedOrphanRun.Head, capacity: ledger);
            var (registered, _) = await RunAsync(sweep, table, context);
            AssertResult(registered, OutcomeKind.Completed, MaintenanceReason.None);
            var run = Assert.Single(runs);
            Assert.Equal(new[] { "old-a.parquet", "old-b.parquet" }, kit.Record(run));
            // The gate's log listing has continuation pages, each rejected
            server.PageSize = _ => 1;
            var rejection = MarkerRejection.AtEveryLogListing(flavor);
            server.Intercept = rejection.Answer;
            var before = server.Requests.Count;

            var declined = await DeleteTurnAsync(run, PinnedOrphanRun.Head);

            Assert.True(rejection.Rejected > 0, "No continuation page of the gate's log listing was requested");
            AssertResult(declined, OutcomeKind.Declined, MaintenanceReason.Transient);
            Assert.Contains("400", declined.Detail);
            Assert.DoesNotContain(server.Requests.Skip(before), x => x.StartsWith("DELETE ", StringComparison.Ordinal));
            Assert.False(run.Ended);
            Assert.Equal(0, run.Cursor);
            // Its reservation is kept
            Assert.True(File.Exists(run.Targets.Path));
            Assert.Equal(CapacityState.Runnable, ledger.StateOf(default, key));
            Assert.NotEqual(default, ledger.Total(CapacityState.Runnable));

            server.Intercept = null;
            var turns = await DeleteTurnsAsync(run, PinnedOrphanRun.Head);

            Assert.All(turns, x => AssertResult(x, OutcomeKind.Completed, MaintenanceReason.None));
            Assert.False(server.Contains("tables/t/old-a.parquet"));
            Assert.False(server.Contains("tables/t/old-b.parquet"));
            Assert.Null(ledger.StateOf(default, key));
            await kit.AssertAsync(runs, 1, contexts: context);
        }
    }
}
