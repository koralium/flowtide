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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Lineage.DataHub.Internal;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using static FlowtideDotNet.Lineage.DataHub.Tests.LineageTestData;

namespace FlowtideDotNet.Lineage.DataHub.Tests
{
    public class DataHubAssertionTests
    {
        private const string DatasetT = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD)";
        private const string DatasetU = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.u,PROD)";

        private static readonly DateTimeOffset Start = new DateTimeOffset(2026, 10, 5, 12, 0, 0, TimeSpan.Zero);

        private sealed class ManualClock : TimeProvider
        {
            public DateTimeOffset Now { get; set; } = Start;

            public override DateTimeOffset GetUtcNow()
            {
                return Now;
            }
        }

        public class CheckedOrder
        {
            public long OrderKey { get; set; }
        }

        // Keys 1 and 20, only 20 fails 'OrderKey < 10'.
        private sealed class OrderSource : GenericDataSourceAsync<CheckedOrder>
        {
            public override TimeSpan? DeltaLoadInterval => null;

            public override async IAsyncEnumerable<FlowtideGenericObject<CheckedOrder>> FullLoadAsync()
            {
                await Task.CompletedTask;
                yield return new FlowtideGenericObject<CheckedOrder>("1", new CheckedOrder() { OrderKey = 1 }, 1, false);
                yield return new FlowtideGenericObject<CheckedOrder>("20", new CheckedOrder() { OrderKey = 20 }, 1, false);
            }
        }

        // Keys 1 and 20, then deletes 20 once asked so the check passes.
        private sealed class FixableOrderSource : GenericDataSourceAsync<CheckedOrder>
        {
            private volatile bool _fixed;

            // Only RunAsync ticks the schedule, the test triggers the delta load itself.
            public override TimeSpan? DeltaLoadInterval => TimeSpan.FromMinutes(10);

            public void Fix()
            {
                _fixed = true;
            }

            public override async IAsyncEnumerable<FlowtideGenericObject<CheckedOrder>> FullLoadAsync()
            {
                await Task.CompletedTask;
                yield return new FlowtideGenericObject<CheckedOrder>("1", new CheckedOrder() { OrderKey = 1 }, 1, false);
                yield return new FlowtideGenericObject<CheckedOrder>("20", new CheckedOrder() { OrderKey = 20 }, 1, false);
            }

            public override async IAsyncEnumerable<FlowtideGenericObject<CheckedOrder>> DeltaLoadAsync(long lastWatermark)
            {
                await Task.CompletedTask;
                if (_fixed && lastWatermark < 2)
                {
                    yield return new FlowtideGenericObject<CheckedOrder>("20", null, 2, true);
                }
            }
        }

        private static StreamLineageCheck Check(string checkId, string message, string[] tables, bool replicated = false)
        {
            return new StreamLineageCheck()
            {
                CheckId = checkId,
                Message = message,
                Targets = tables.Select(x => new StreamLineageCheckTarget(x, "postgres", x, x.Split('.'))).ToList(),
                Replicated = replicated
            };
        }

        // Stream writing t and u from s.
        private static StreamLineage Lineage(IReadOnlyList<StreamLineageCheck> checks, string? substream = null)
        {
            return Snapshot(
                [Input("postgres", "s", [Col("x", new Int64Type())])],
                [
                    Output("postgres", "t", [Col("x", new Int64Type())], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"]),
                    Output("postgres", "u", [Col("x", new Int64Type())], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])
                ],
                substream).WithChecks(checks);
        }

        private static DataHubLineageStore Store(ManualClock clock, Action<DataHubLineageOptions>? configure = null)
        {
            var options = new DataHubLineageOptions();
            options.MapNamespace("postgres", m =>
            {
                m.Database = "db";
                m.DefaultSchema = "public";
            });
            configure?.Invoke(options);
            return new DataHubLineageStore(options, clock);
        }

        // Computed apart from the package, so the urn format is pinned.
        private static string AssertionUrn(string stream, string datasetUrn, string message, int ordinal)
        {
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{stream}\u001f{datasetUrn}\u001f{message}\u001f{ordinal}"));
            return "urn:li:assertion:" + Convert.ToHexString(hash, 0, 16).ToLowerInvariant();
        }

        private static List<string> AssertionUrns(DataHubSnapshot snapshot)
        {
            return snapshot.Urns.Where(x => x.StartsWith("urn:li:assertion:", StringComparison.Ordinal)).ToList();
        }

        private static JsonElement Aspects(DataHubLineageStore store, string urn)
        {
            Assert.True(store.GetSnapshot().TryGetEntity(urn, out var json), urn);
            return JsonDocument.Parse(json).RootElement.GetProperty("aspects");
        }

        private static JsonElement RunEvent(DataHubLineageStore store, string urn)
        {
            var runEvent = Aspects(store, urn).GetProperty("assertionRunEvent");
            Assert.Equal("TIMESERIES", runEvent.GetProperty("type").GetString());
            return runEvent.GetProperty("value");
        }

        private static (string Type, long UnexpectedCount, string ActiveIssues, long TimestampMillis) Result(DataHubLineageStore store, string urn)
        {
            var runEvent = RunEvent(store, urn);
            var result = runEvent.GetProperty("result");
            return (
                result.GetProperty("type").GetString()!,
                result.GetProperty("unexpectedCount").GetInt64(),
                result.GetProperty("nativeResults").GetProperty("activeIssues").GetString()!,
                runEvent.GetProperty("timestampMillis").GetInt64());
        }

        // Single quotes keep the golden JSON readable.
        private static string Json(string text)
        {
            return text.Replace('\'', '"');
        }

        [Fact]
        public void CheckIsAnAssertionOnEveryTargetDataset()
        {
            var store = Store(new ManualClock());
            store.Register(Lineage([Check("1:0", "x is negative", ["t", "u"])]), "a");

            var urnT = AssertionUrn("a", DatasetT, "x is negative", 0);
            var urnU = AssertionUrn("a", DatasetU, "x is negative", 0);
            Assert.Equal(new[] { urnT, urnU }.Order(StringComparer.Ordinal), AssertionUrns(store.GetSnapshot()));
            var aspects = Aspects(store, urnT);
            Assert.Equal(["assertionInfo", "dataPlatformInstance", "status"], aspects.EnumerateObject().Select(x => x.Name));
            Assert.Equal(
                Json($"{{'customProperties':{{'flowtide.stream':'a','flowtide.checkIds':'1:0'}},'type':'CUSTOM','customAssertion':{{'type':'Flowtide Check','entity':'{DatasetT}'}},'source':{{'type':'EXTERNAL'}},'description':'x is negative','entityUrn':'{DatasetT}'}}"),
                aspects.GetProperty("assertionInfo").GetProperty("value").GetRawText());
            Assert.Equal(Json("{'platform':'urn:li:dataPlatform:flowtide'}"), aspects.GetProperty("dataPlatformInstance").GetProperty("value").GetRawText());
            Assert.Equal(Json("{'removed':false}"), aspects.GetProperty("status").GetProperty("value").GetRawText());
        }

        [Theory]
        [InlineData(CheckState.NotEvaluated, "SUCCESS", 0L, 0L)]
        [InlineData(CheckState.Passed, "SUCCESS", 0L, 0L)]
        [InlineData(CheckState.Failed, "FAILURE", 2L, 5L)]
        public void StatusIsServedAsARunEvent(CheckState state, string type, long activeIssues, long failingRows)
        {
            var store = Store(new ManualClock());
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);

            store.RecordCheckStatus(a, "1:0", state, activeIssues, failingRows);

            Assert.Equal(
                Json($"{{'timestampMillis':{Start.ToUnixTimeMilliseconds()},'runId':'a','asserteeUrn':'{DatasetT}','status':'COMPLETE','result':{{'type':'{type}','unexpectedCount':{failingRows},'nativeResults':{{'activeIssues':'{activeIssues}','failingRows':'{failingRows}'}}}},'assertionUrn':'{urn}','partitionSpec':{{'type':'FULL_TABLE','partition':'FULL_TABLE_SNAPSHOT'}}}}"),
                RunEvent(store, urn).GetRawText());
        }

        [Fact]
        public void StatusOfAReplacedBuildIsIgnored()
        {
            var store = Store(new ManualClock());
            var replaced = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");

            // The replaced build may still be stopping.
            store.RecordCheckStatus(replaced, "1:0", CheckState.Failed, 1, 1);

            Assert.False(Aspects(store, AssertionUrn("a", DatasetT, "x is negative", 0)).TryGetProperty("assertionRunEvent", out _));
        }

        [Fact]
        public void TimestampChangesOnlyWithTheStatus()
        {
            var clock = new ManualClock();
            var store = Store(clock);
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);
            var snapshot = store.GetSnapshot();
            var start = Start.ToUnixTimeMilliseconds();

            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal(("FAILURE", 1L, "1", start), Result(store, urn));

            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 2);
            Assert.Equal(("FAILURE", 2L, "1", start + 60_000), Result(store, urn));

            // Same millisecond, DataHub only summarizes newer events.
            store.RecordCheckStatus(a, "1:0", CheckState.Passed, 0, 0);
            Assert.Equal(("SUCCESS", 0L, "0", start + 60_001), Result(store, urn));

            // A clock going back never reuses a timestamp.
            clock.Now = Start;
            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 3, 3);
            Assert.Equal(("FAILURE", 3L, "3", start + 60_002), Result(store, urn));

            // The status is read per request, the snapshot stays cached.
            Assert.Same(snapshot, store.GetSnapshot());
        }

        [Fact]
        public void PartitionCopiesShareOneAssertion()
        {
            var store = Store(new ManualClock());
            var sub0 = store.Register(Lineage([Check("sub0/1:0", "x is negative", ["t"])], "sub0"), "a");
            var sub1 = store.Register(Lineage([Check("sub1/1:0", "x is negative", ["t"])], "sub1"), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);

            Assert.Equal([urn], AssertionUrns(store.GetSnapshot()));
            Assert.Equal("sub0/1:0,sub1/1:0", Aspects(store, urn).GetProperty("assertionInfo").GetProperty("value").GetProperty("customProperties").GetProperty("flowtide.checkIds").GetString());
            Assert.False(Aspects(store, urn).TryGetProperty("assertionRunEvent", out _));

            // A part that has not reported is left out.
            store.RecordCheckStatus(sub0, "sub0/1:0", CheckState.Passed, 0, 0);
            var passed = Result(store, urn);
            Assert.Equal("SUCCESS", passed.Type);

            // Not evaluated counts as passed, so the served event stays the same.
            store.RecordCheckStatus(sub1, "sub1/1:0", CheckState.NotEvaluated, 0, 0);
            Assert.Equal(passed, Result(store, urn));

            // Rows add up, an issue active in both partitions counts once.
            store.RecordCheckStatus(sub0, "sub0/1:0", CheckState.Failed, 2, 3);
            store.RecordCheckStatus(sub1, "sub1/1:0", CheckState.Failed, 1, 4);
            var (type, unexpectedCount, activeIssues, _) = Result(store, urn);
            Assert.Equal(("FAILURE", 7L, "2"), (type, unexpectedCount, activeIssues));
        }

        [Fact]
        public void RebuildKeepsTheLastResultUntilTheRebuiltPartReports()
        {
            var clock = new ManualClock();
            var store = Store(clock);
            var sub0 = store.Register(Lineage([Check("sub0/1:0", "x is negative", ["t"])], "sub0"), "a");
            var sub1 = store.Register(Lineage([Check("sub1/1:0", "x is negative", ["t"])], "sub1"), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);
            store.RecordCheckStatus(sub1, "sub1/1:0", CheckState.Passed, 0, 0);
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(sub0, "sub0/1:0", CheckState.Failed, 1, 1);
            var failure = Result(store, urn);
            Assert.Equal("FAILURE", failure.Type);

            // Only the older passing copy is left, the failure is still the newest status.
            var rebuilt = store.Register(Lineage([Check("sub0/2:0", "x is negative", ["t"])], "sub0"), "a");
            Assert.Equal(failure, Result(store, urn));

            clock.Now = Start.AddMinutes(2);
            store.RecordCheckStatus(rebuilt, "sub0/2:0", CheckState.Passed, 0, 0);
            Assert.Equal(("SUCCESS", 0L, "0", Start.AddMinutes(2).ToUnixTimeMilliseconds()), Result(store, urn));
        }

        [Fact]
        public void CheckIdReusedByARebuildReportsFresh()
        {
            var clock = new ManualClock();
            var store = Store(clock);
            var first = store.Register(Lineage([Check("1:0", "x is negative", ["t"]), Check("2:0", "y is null", ["t"])]), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);
            store.RecordCheckStatus(first, "2:0", CheckState.Failed, 1, 1);
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(first, "1:0", CheckState.Passed, 0, 0);
            Assert.Equal("SUCCESS", Result(store, urn).Type);

            // The rebuilt plan gives the x check the id the y check had, with the same counts.
            var rebuilt = store.Register(Lineage([Check("2:0", "x is negative", ["t"])]), "a");
            clock.Now = Start.AddMinutes(2);
            store.RecordCheckStatus(rebuilt, "2:0", CheckState.Failed, 1, 1);

            Assert.Equal(("FAILURE", 1L, "1", Start.AddMinutes(2).ToUnixTimeMilliseconds()), Result(store, urn));
        }

        [Fact]
        public void ObsoleteSnapshotNeverHidesTheCurrentResult()
        {
            var clock = new ManualClock();
            var store = Store(clock);
            var sub0 = store.Register(Lineage([Check("sub0/1:0", "x is negative", ["t"])], "sub0"), "a");
            var sub1 = store.Register(Lineage([Check("sub1/1:0", "x is negative", ["t"])], "sub1"), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);
            store.RecordCheckStatus(sub0, "sub0/1:0", CheckState.Passed, 0, 0);
            store.RecordCheckStatus(sub1, "sub1/1:0", CheckState.Failed, 1, 1);
            var obsolete = store.GetSnapshot();

            var rebuilt = store.Register(Lineage([Check("sub0/1:0", "x is negative", ["t"])], "sub0"), "a");
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(rebuilt, "sub0/1:0", CheckState.Failed, 1, 1);
            clock.Now = Start.AddMinutes(2);
            store.RecordCheckStatus(sub1, "sub1/1:0", CheckState.Passed, 0, 0);
            // A request that took the snapshot before the rebuild only sees the passing copy.
            Assert.True(obsolete.TryGetEntity(urn, out _));

            Assert.Equal(("FAILURE", 1L, "1", Start.AddMinutes(2).ToUnixTimeMilliseconds()), Result(store, urn));
        }

        [Fact]
        public void RebuildWithoutAnyReportedPartKeepsTheLastResult()
        {
            var store = Store(new ManualClock());
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);
            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);
            var failure = Result(store, urn);

            store.Register(Lineage([Check("2:0", "x is negative", ["t"])]), "a");

            Assert.Equal(failure, Result(store, urn));
        }

        [Fact]
        public void ObsoleteSnapshotServesTheLastPublishedResult()
        {
            var clock = new ManualClock();
            var store = Store(clock);
            var first = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);
            store.RecordCheckStatus(first, "1:0", CheckState.Failed, 1, 1);
            var obsolete = store.GetSnapshot();
            var failure = Result(store, urn);

            var rebuilt = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(rebuilt, "1:0", CheckState.Passed, 0, 0);
            Assert.True(obsolete.TryGetEntity(urn, out var json));

            Assert.Equal(failure.TimestampMillis, JsonDocument.Parse(json).RootElement.GetProperty("aspects").GetProperty("assertionRunEvent").GetProperty("value").GetProperty("timestampMillis").GetInt64());
            Assert.Equal("SUCCESS", Result(store, urn).Type);
        }

        [Fact]
        public void ReplicatedCopiesCountTheirRowsOnce()
        {
            var store = Store(new ManualClock());
            var sub0 = store.Register(Lineage([Check("sub0/1:0", "x is negative", ["t"], replicated: true)], "sub0"), "a");
            var sub1 = store.Register(Lineage([Check("sub1/1:0", "x is negative", ["t"], replicated: true)], "sub1"), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);

            store.RecordCheckStatus(sub0, "sub0/1:0", CheckState.Failed, 1, 5);
            store.RecordCheckStatus(sub1, "sub1/1:0", CheckState.Failed, 1, 5);

            Assert.Equal(5L, Result(store, urn).UnexpectedCount);
        }

        [Fact]
        public void ChecksWithTheSameMessageAreToldApartByOrder()
        {
            var store = Store(new ManualClock());
            var a = store.Register(Lineage([Check("1:0", "bad row", ["t"]), Check("2:0", "bad row", ["t"])]), "a");
            var first = AssertionUrn("a", DatasetT, "bad row", 0);
            var second = AssertionUrn("a", DatasetT, "bad row", 1);

            store.RecordCheckStatus(a, "1:0", CheckState.Passed, 0, 0);
            store.RecordCheckStatus(a, "2:0", CheckState.Failed, 1, 1);

            Assert.Equal(new[] { first, second }.Order(StringComparer.Ordinal), AssertionUrns(store.GetSnapshot()));
            Assert.Equal("SUCCESS", Result(store, first).Type);
            Assert.Equal("FAILURE", Result(store, second).Type);
        }

        [Fact]
        public void ExcludedTargetHasNoAssertion()
        {
            var store = Store(new ManualClock(), o => o.ExcludedNamespaces.Add("other"));
            var lineage = Lineage([new StreamLineageCheck()
            {
                CheckId = "1:0",
                Message = "x is negative",
                Targets = [new StreamLineageCheckTarget("t", "postgres", "t", ["t"]), new StreamLineageCheckTarget("o", "other", "o", ["o"])]
            }]);
            store.Register(lineage, "a");

            Assert.Equal([AssertionUrn("a", DatasetT, "x is negative", 0)], AssertionUrns(store.GetSnapshot()));
        }

        [Fact]
        public void IncludeChecksFalseServesNoAssertions()
        {
            var store = Store(new ManualClock(), o => o.IncludeChecks = false);
            store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");

            Assert.Empty(AssertionUrns(store.GetSnapshot()));
            Assert.NotEmpty(store.GetSnapshot().Urns);
        }

        [Fact]
        public void AspectProviderSeesAssertions()
        {
            var contexts = new List<DataHubEntityContext>();
            var store = Store(new ManualClock(), o => o.AspectProvider = context =>
            {
                if (context.EntityType != DataHubEntityType.Assertion)
                {
                    return null;
                }
                contexts.Add(context);
                return [new DataHubAspect("globalTags", JsonNode.Parse(Json("{'tags':[{'tag':'urn:li:tag:quality'}]}"))!)];
            });
            store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var urn = AssertionUrn("a", DatasetT, "x is negative", 0);

            var tags = Aspects(store, urn).GetProperty("globalTags").GetProperty("value").GetRawText();

            Assert.Equal(Json("{'tags':[{'tag':'urn:li:tag:quality'}]}"), tags);
            var context = Assert.Single(contexts);
            Assert.Equal((urn, "a", "postgres", "t"), (context.Urn, context.StreamName, context.Namespace, context.TableName));
        }

        // Computed apart from the package, so the urn format is pinned.
        private static string IncidentUrn(string assertionUrn)
        {
            return "urn:li:incident:" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(assertionUrn)), 0, 16).ToLowerInvariant();
        }

        private static string? IncidentText(DataHubLineageStore store, string urn)
        {
            return store.GetSnapshot().TryGetEntity(urn, out var json) ? Encoding.UTF8.GetString(json.Span) : null;
        }

        private static JsonElement IncidentInfo(DataHubLineageStore store, string urn)
        {
            var aspects = Aspects(store, urn);
            Assert.Equal(["incidentInfo"], aspects.EnumerateObject().Select(x => x.Name));
            return aspects.GetProperty("incidentInfo").GetProperty("value");
        }

        private static (string State, long LastUpdated, long StartedAt, long Created) IncidentState(DataHubLineageStore store, string urn)
        {
            var info = IncidentInfo(store, urn);
            var status = info.GetProperty("status");
            return (
                status.GetProperty("state").GetString()!,
                status.GetProperty("lastUpdated").GetProperty("time").GetInt64(),
                info.GetProperty("startedAt").GetInt64(),
                info.GetProperty("created").GetProperty("time").GetInt64());
        }

        [Fact]
        public void IncidentsAreOffByDefault()
        {
            var store = Store(new ManualClock());
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");

            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);

            Assert.DoesNotContain(store.GetSnapshot().Urns, x => x.StartsWith("urn:li:incident:", StringComparison.Ordinal));
        }

        [Fact]
        public void FailingCheckRaisesAnIncident()
        {
            var store = Store(new ManualClock(), o => o.RaiseIncidents = true);
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var assertion = AssertionUrn("a", DatasetT, "x is negative", 0);
            var incident = IncidentUrn(assertion);
            var start = Start.ToUnixTimeMilliseconds();

            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);

            Assert.Contains(incident, store.GetSnapshot().Urns);
            var info = JsonNode.Parse(IncidentInfo(store, incident).GetRawText())!.AsObject();
            Assert.Equal("Raised by the Flowtide check 'x is negative' in stream 'a'.", info["description"]!.GetValue<string>());
            info.Remove("description");
            Assert.Equal(
                Json($"{{'type':'CUSTOM','customType':'Flowtide Check','title':'x is negative','entities':['{DatasetT}'],'priority':2,'status':{{'state':'ACTIVE','lastUpdated':{{'time':{start},'actor':'urn:li:corpuser:flowtide'}}}},'source':{{'type':'ASSERTION_FAILURE','sourceUrn':'{assertion}'}},'startedAt':{start},'created':{{'time':{start},'actor':'urn:li:corpuser:flowtide'}}}}"),
                info.ToJsonString());
        }

        [Fact]
        public void IncidentIsServedOnlyOnceItsCheckFailed()
        {
            var store = Store(new ManualClock(), o => o.RaiseIncidents = true);
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var incident = IncidentUrn(AssertionUrn("a", DatasetT, "x is negative", 0));

            Assert.Null(IncidentText(store, incident));
            store.RecordCheckStatus(a, "1:0", CheckState.NotEvaluated, 0, 0);
            Assert.Null(IncidentText(store, incident));
            store.RecordCheckStatus(a, "1:0", CheckState.Passed, 0, 0);

            Assert.Null(IncidentText(store, incident));
            Assert.Contains(incident, store.GetSnapshot().Urns);
        }

        [Fact]
        public void PassingCheckResolvesAndAFailureReopensTheIncident()
        {
            var clock = new ManualClock();
            var store = Store(clock, o => o.RaiseIncidents = true);
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var incident = IncidentUrn(AssertionUrn("a", DatasetT, "x is negative", 0));
            var start = Start.ToUnixTimeMilliseconds();
            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal(("ACTIVE", start, start, start), IncidentState(store, incident));

            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(a, "1:0", CheckState.Passed, 0, 0);
            var resolved = IncidentText(store, incident);
            Assert.Equal(("RESOLVED", start + 60_000, start, start), IncidentState(store, incident));
            Assert.Equal("Resolved by Flowtide, the check reports no failing rows.", IncidentInfo(store, incident).GetProperty("status").GetProperty("message").GetString());

            // Unchanged state keeps the bytes, DataHub then writes nothing.
            clock.Now = Start.AddMinutes(2);
            Assert.Equal(resolved, IncidentText(store, incident));

            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 2, 2);
            Assert.Equal(("ACTIVE", start + 120_000, start + 120_000, start), IncidentState(store, incident));
            Assert.False(IncidentInfo(store, incident).GetProperty("status").TryGetProperty("message", out _));
        }

        [Fact]
        public void RemovedCheckResolvesItsIncident()
        {
            var clock = new ManualClock();
            var store = Store(clock, o => o.RaiseIncidents = true);
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var incident = IncidentUrn(AssertionUrn("a", DatasetT, "x is negative", 0));
            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal("ACTIVE", IncidentState(store, incident).State);

            clock.Now = Start.AddMinutes(1);
            store.Register(Lineage([Check("1:0", "y is null", ["t"])]), "a");

            Assert.Contains(incident, store.GetSnapshot().Urns);
            Assert.Equal(("RESOLVED", Start.AddMinutes(1).ToUnixTimeMilliseconds()), (IncidentState(store, incident).State, IncidentState(store, incident).LastUpdated));
            Assert.Equal("Resolved by Flowtide, the check was removed from the stream.", IncidentInfo(store, incident).GetProperty("status").GetProperty("message").GetString());
        }

        [Fact]
        public void ReturningCheckReopensOnlyOnANewFailure()
        {
            var clock = new ManualClock();
            var store = Store(clock, o => o.RaiseIncidents = true);
            var first = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var incident = IncidentUrn(AssertionUrn("a", DatasetT, "x is negative", 0));
            store.RecordCheckStatus(first, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal("ACTIVE", IncidentState(store, incident).State);
            store.Register(Lineage([Check("1:0", "y is null", ["t"])]), "a");
            Assert.Equal("RESOLVED", IncidentState(store, incident).State);

            // The old failure is still the last served result until the returned check reports.
            var returned = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            Assert.Equal("RESOLVED", IncidentState(store, incident).State);

            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(returned, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal(("ACTIVE", Start.AddMinutes(1).ToUnixTimeMilliseconds()), (IncidentState(store, incident).State, IncidentState(store, incident).StartedAt));
        }

        [Fact]
        public void IncidentFollowsTheServedResultAfterARebuildDropsACopy()
        {
            var clock = new ManualClock();
            var store = Store(clock, o => o.RaiseIncidents = true);
            var sub0 = store.Register(Lineage([Check("sub0/1:0", "x is negative", ["t"])], "sub0"), "a");
            var sub1 = store.Register(Lineage([Check("sub1/1:0", "x is negative", ["t"])], "sub1"), "a");
            var assertion = AssertionUrn("a", DatasetT, "x is negative", 0);
            var incident = IncidentUrn(assertion);
            store.RecordCheckStatus(sub0, "sub0/1:0", CheckState.Failed, 1, 1);
            Assert.Equal("FAILURE", Result(store, assertion).Type);

            // The other copy reports between the assertion and the incident fetch of one run.
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(sub1, "sub1/1:0", CheckState.NotEvaluated, 0, 0);
            Assert.Equal("ACTIVE", IncidentState(store, incident).State);

            store.Register(Lineage([], "sub0"), "a");

            Assert.Equal("SUCCESS", Result(store, assertion).Type);
            Assert.Equal(("RESOLVED", Start.AddMinutes(1).ToUnixTimeMilliseconds()), (IncidentState(store, incident).State, IncidentState(store, incident).LastUpdated));
        }

        [Fact]
        public void IncidentFollowsAFailureServedBeforeARebuild()
        {
            var clock = new ManualClock();
            var store = Store(clock, o => o.RaiseIncidents = true);
            var first = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var assertion = AssertionUrn("a", DatasetT, "x is negative", 0);
            var incident = IncidentUrn(assertion);
            store.RecordCheckStatus(first, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal("ACTIVE", IncidentState(store, incident).State);
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(first, "1:0", CheckState.Passed, 0, 0);
            Assert.Equal("RESOLVED", IncidentState(store, incident).State);

            clock.Now = Start.AddMinutes(2);
            store.RecordCheckStatus(first, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal("FAILURE", Result(store, assertion).Type);
            // Rebuilt between the assertion and the incident fetch, the new build has not reported yet.
            store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");

            Assert.Equal(("ACTIVE", Start.AddMinutes(2).ToUnixTimeMilliseconds()), (IncidentState(store, incident).State, IncidentState(store, incident).StartedAt));
        }

        [Fact]
        public void ReturningPassingCheckIsNoLongerReportedAsRemoved()
        {
            var clock = new ManualClock();
            var store = Store(clock, o => o.RaiseIncidents = true);
            var first = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var incident = IncidentUrn(AssertionUrn("a", DatasetT, "x is negative", 0));
            store.RecordCheckStatus(first, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal("ACTIVE", IncidentState(store, incident).State);
            store.Register(Lineage([Check("1:0", "y is null", ["t"])]), "a");
            Assert.Equal("RESOLVED", IncidentState(store, incident).State);

            var returned = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            clock.Now = Start.AddMinutes(1);
            store.RecordCheckStatus(returned, "1:0", CheckState.Passed, 0, 0);

            Assert.Equal(("RESOLVED", Start.AddMinutes(1).ToUnixTimeMilliseconds()), (IncidentState(store, incident).State, IncidentState(store, incident).LastUpdated));
            Assert.Equal("Resolved by Flowtide, the check reports no failing rows.", IncidentInfo(store, incident).GetProperty("status").GetProperty("message").GetString());
        }

        [Fact]
        public void ObsoleteSnapshotNeverRaisesAnIncident()
        {
            var store = Store(new ManualClock(), o => o.RaiseIncidents = true);
            var first = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var assertion = AssertionUrn("a", DatasetT, "x is negative", 0);
            var incident = IncidentUrn(assertion);
            store.RecordCheckStatus(first, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal("FAILURE", Result(store, assertion).Type);
            var obsolete = store.GetSnapshot();

            // The rebuilt stream has no such check, and its snapshot knows no incident to resolve.
            store.Register(Lineage([Check("1:0", "y is null", ["t"])]), "a");
            Assert.DoesNotContain(incident, store.GetSnapshot().Urns);

            Assert.False(obsolete.TryGetEntity(incident, out _));
            Assert.Null(IncidentText(store, incident));
        }

        [Fact]
        public void IncidentPriorityFollowsTheResolverAfterARebuild()
        {
            var priority = DataHubIncidentPriority.Low;
            var store = Store(new ManualClock(), o =>
            {
                o.RaiseIncidents = true;
                o.IncidentPriorityResolver = _ => priority;
            });
            var first = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            var incident = IncidentUrn(AssertionUrn("a", DatasetT, "x is negative", 0));
            store.RecordCheckStatus(first, "1:0", CheckState.Failed, 1, 1);
            Assert.Equal(3, IncidentInfo(store, incident).GetProperty("priority").GetInt32());

            priority = DataHubIncidentPriority.Critical;
            var rebuilt = store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            store.RecordCheckStatus(rebuilt, "1:0", CheckState.Failed, 1, 1);

            Assert.Equal(0, IncidentInfo(store, incident).GetProperty("priority").GetInt32());
        }

        [Fact]
        public void IncidentPriorityComesFromTheOptionsOrTheResolver()
        {
            var contexts = new List<DataHubIncidentContext>();
            var store = Store(new ManualClock(), o =>
            {
                o.RaiseIncidents = true;
                o.IncidentPriority = DataHubIncidentPriority.Critical;
                o.IncidentPriorityResolver = context =>
                {
                    contexts.Add(context);
                    return context.CheckMessage == "y is null" ? DataHubIncidentPriority.Low : null;
                };
            });
            var a = store.Register(Lineage([Check("1:0", "x is negative", ["t"]), Check("2:0", "y is null", ["t"])]), "a");
            var x = AssertionUrn("a", DatasetT, "x is negative", 0);
            var y = AssertionUrn("a", DatasetT, "y is null", 0);
            store.RecordCheckStatus(a, "1:0", CheckState.Failed, 1, 1);
            store.RecordCheckStatus(a, "2:0", CheckState.Failed, 1, 1);

            Assert.Equal(0, IncidentInfo(store, IncidentUrn(x)).GetProperty("priority").GetInt32());
            Assert.Equal(3, IncidentInfo(store, IncidentUrn(y)).GetProperty("priority").GetInt32());
            var context = Assert.Single(contexts, c => c.CheckMessage == "y is null");
            Assert.Equal(("a", y, DatasetT, "postgres", "t"), (context.StreamName, context.AssertionUrn, context.DatasetUrn, context.Namespace, context.TableName));
        }

        [Fact]
        public void InvalidIncidentSettingsFail()
        {
            Assert.Throws<ArgumentException>(() => Store(new ManualClock(), o => o.IncidentPriority = (DataHubIncidentPriority)9));
            Assert.Throws<ArgumentException>(() => Store(new ManualClock(), o =>
            {
                o.RaiseIncidents = true;
                o.IncludeChecks = false;
            }));

            // DataHub stores any integer, but shows no priority outside 0 to 3.
            var store = Store(new ManualClock(), o =>
            {
                o.RaiseIncidents = true;
                o.IncidentPriorityResolver = _ => (DataHubIncidentPriority)9;
            });
            store.Register(Lineage([Check("1:0", "x is negative", ["t"])]), "a");
            Assert.Throws<InvalidOperationException>(() => store.GetSnapshot());
        }

        [Fact]
        public async Task RunningStreamRaisesAnIncident()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddFlowtideDataHubLineage(o =>
            {
                o.RaiseIncidents = true;
                o.IncidentPriority = DataHubIncidentPriority.High;
            });
            services.AddFlowtideStream("checked")
                .AddSqlTextAsPlan("INSERT INTO output SELECT CHECK_VALUE(OrderKey, OrderKey < 10, 'order key too large') AS OrderKey FROM orders")
                .AddConnectors(c =>
                {
                    c.AddCustomSource("orders", _ => new OrderSource());
                    c.AddBlackholeSink("*");
                })
                .AddStorage(s => s.AddTemporaryDevelopmentStorage(o => o.DirectoryPath = $"./data/tempFiles/datahubincidents{Guid.NewGuid():N}"))
                .AddDataHubLineage();
            await using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();
            var stream = provider.GetRequiredKeyedService<FlowtideDotNet.Base.Engine.DataflowStream>("checked");
            var incident = IncidentUrn(AssertionUrn("checked", "urn:li:dataset:(urn:li:dataPlatform:blackhole,output,PROD)", "order key too large", 0));
            Assert.Null(IncidentText(store, incident));

            await stream.StartAsync();
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(60) && IncidentText(store, incident) == null)
            {
                await Task.Delay(10);
            }

            Assert.Equal("ACTIVE", IncidentState(store, incident).State);
            Assert.Equal(1, IncidentInfo(store, incident).GetProperty("priority").GetInt32());
        }

        [Fact]
        public async Task RunningStreamResolvesTheIncidentWhenTheFailingRowIsGone()
        {
            var source = new FixableOrderSource();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddFlowtideDataHubLineage(o => o.RaiseIncidents = true);
            services.AddFlowtideStream("checked")
                .AddSqlTextAsPlan("INSERT INTO output SELECT CHECK_VALUE(OrderKey, OrderKey < 10, 'order key too large') AS OrderKey FROM orders")
                .AddConnectors(c =>
                {
                    c.AddCustomSource("orders", _ => source);
                    c.AddBlackholeSink("*");
                })
                .AddStorage(s => s.AddTemporaryDevelopmentStorage(o => o.DirectoryPath = $"./data/tempFiles/datahubresolve{Guid.NewGuid():N}"))
                .AddDataHubLineage();
            await using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();
            var stream = provider.GetRequiredKeyedService<FlowtideDotNet.Base.Engine.DataflowStream>("checked");
            var assertion = AssertionUrn("checked", "urn:li:dataset:(urn:li:dataPlatform:blackhole,output,PROD)", "order key too large", 0);
            var incident = IncidentUrn(assertion);

            await stream.StartAsync();
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(60) && IncidentText(store, incident) == null)
            {
                await Task.Delay(10);
            }
            Assert.Equal("ACTIVE", IncidentState(store, incident).State);

            source.Fix();
            await stream.CallTrigger("delta_load", null);
            stopwatch.Restart();
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(60) && IncidentState(store, incident).State != "RESOLVED")
            {
                await Task.Delay(10);
            }

            Assert.Equal("RESOLVED", IncidentState(store, incident).State);
            Assert.Equal(("SUCCESS", 0L), (Result(store, assertion).Type, Result(store, assertion).UnexpectedCount));
        }

        [Fact]
        public async Task RunningStreamReportsItsCheckStatus()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddFlowtideStream("checked")
                .AddSqlTextAsPlan("INSERT INTO output SELECT CHECK_VALUE(OrderKey, OrderKey < 10, 'order key too large') AS OrderKey FROM orders")
                .AddConnectors(c =>
                {
                    c.AddCustomSource("orders", _ => new OrderSource());
                    c.AddBlackholeSink("*");
                })
                .AddStorage(s => s.AddTemporaryDevelopmentStorage(o => o.DirectoryPath = $"./data/tempFiles/datahubchecks{Guid.NewGuid():N}"))
                .AddDataHubLineage();
            await using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();
            var stream = provider.GetRequiredKeyedService<FlowtideDotNet.Base.Engine.DataflowStream>("checked");
            var urn = AssertionUrn("checked", "urn:li:dataset:(urn:li:dataPlatform:blackhole,output,PROD)", "order key too large", 0);
            Assert.Equal([urn], AssertionUrns(store.GetSnapshot()));

            await stream.StartAsync();
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(60) &&
                !(Aspects(store, urn).TryGetProperty("assertionRunEvent", out var runEvent) && runEvent.GetProperty("value").GetProperty("result").GetProperty("type").GetString() == "FAILURE"))
            {
                await Task.Delay(10);
            }

            var (type, unexpectedCount, activeIssues, _) = Result(store, urn);
            Assert.Equal(("FAILURE", 1L, "1"), (type, unexpectedCount, activeIssues));
        }
    }
}
