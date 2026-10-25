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
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using static FlowtideDotNet.Lineage.DataHub.Tests.LineageTestData;

namespace FlowtideDotNet.Lineage.DataHub.Tests
{
    public class DataHubRunTests
    {
        private const string Job = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),postgres.db.public.t)";
        private const string DatasetS = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.s,PROD)";
        private const string DatasetT = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD)";

        private static readonly DateTimeOffset Start = new DateTimeOffset(2026, 10, 6, 12, 0, 0, TimeSpan.Zero);

        private sealed class ManualClock : TimeProvider
        {
            public DateTimeOffset Now { get; set; } = Start;

            public override DateTimeOffset GetUtcNow()
            {
                return Now;
            }
        }

        public class Row
        {
            public long Key { get; set; }
        }

        private sealed class RowSource : GenericDataSourceAsync<Row>
        {
            public override TimeSpan? DeltaLoadInterval => null;

            public override async IAsyncEnumerable<FlowtideGenericObject<Row>> FullLoadAsync()
            {
                await Task.CompletedTask;
                yield return new FlowtideGenericObject<Row>("1", new Row() { Key = 1 }, 1, false);
            }
        }

        // Blocks its initialization, or fails its checkpoints, when asked.
        private sealed class ControlledSource : GenericDataSourceAsync<Row>
        {
            public TaskCompletionSource InitEntered { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource InitGate { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public volatile bool BlockInit;

            public volatile bool FailCheckpoints;

            public override TimeSpan? DeltaLoadInterval => null;

            public override async Task Initialize(ReadRelation readRelation, IStateManagerClient stateManagerClient)
            {
                if (BlockInit)
                {
                    InitEntered.TrySetResult();
                    await InitGate.Task;
                }
            }

            public override Task Checkpoint()
            {
                return FailCheckpoints ? throw new InvalidOperationException("checkpoint failure") : Task.CompletedTask;
            }

            public override async IAsyncEnumerable<FlowtideGenericObject<Row>> FullLoadAsync()
            {
                await Task.CompletedTask;
                yield return new FlowtideGenericObject<Row>("1", new Row() { Key = 1 }, 1, false);
            }
        }

        private static (ServiceProvider Provider, DataHubLineageStore Store, FlowtideDotNet.Base.Engine.DataflowStream Stream) BuildStream(string name, GenericDataSourceAsync<Row> source)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddFlowtideStream(name)
                .AddSqlTextAsPlan("INSERT INTO output SELECT Key FROM rows")
                .AddConnectors(c =>
                {
                    c.AddCustomSource("rows", _ => source);
                    c.AddBlackholeSink("*");
                })
                .AddStorage(s => s.AddTemporaryDevelopmentStorage(o => o.DirectoryPath = $"./data/tempFiles/datahubruns{Guid.NewGuid():N}"))
                .AddDataHubLineage();
            var provider = services.BuildServiceProvider();
            return (provider, provider.GetRequiredService<DataHubLineageStore>(), provider.GetRequiredKeyedService<FlowtideDotNet.Base.Engine.DataflowStream>(name));
        }

        private static async Task WaitFor(Func<bool> condition)
        {
            var stopwatch = Stopwatch.StartNew();
            while (!condition() && stopwatch.Elapsed < TimeSpan.FromSeconds(60))
            {
                await Task.Delay(10);
            }
        }

        // Stream writing t from s, and an excluded table.
        private static StreamLineage Lineage(string? substream = null)
        {
            return Snapshot(
                [Input("postgres", "s", [Col("x", new Int64Type())]), Input("console", "c", [Col("x", new Int64Type())])],
                [Output("postgres", "t", [Col("x", new Int64Type())], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])],
                substream);
        }

        private static DataHubLineageStore Store(ManualClock clock, Action<DataHubLineageOptions>? configure = null)
        {
            var options = new DataHubLineageOptions();
            options.ExcludedNamespaces.Add("console");
            options.MapNamespace("postgres", m =>
            {
                m.Database = "db";
                m.DefaultSchema = "public";
            });
            configure?.Invoke(options);
            return new DataHubLineageStore(options, clock);
        }

        // Computed apart from the package, so the urn format is pinned.
        private static string RunUrn(string stream, string? substream = null, string jobId = "postgres.db.public.t")
        {
            var job = $"urn:li:dataJob:(urn:li:dataFlow:(flowtide,{stream},PROD),{jobId})";
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"flowtide\u001f{job}\u001f{substream}"));
            return "urn:li:dataProcessInstance:" + Convert.ToHexString(hash, 0, 16).ToLowerInvariant();
        }

        private static List<string> RunUrns(DataHubLineageStore store)
        {
            return store.GetSnapshot().Urns.Where(x => x.StartsWith("urn:li:dataProcessInstance:", StringComparison.Ordinal)).ToList();
        }

        private static string? Text(DataHubLineageStore store, string urn)
        {
            return store.GetSnapshot().TryGetEntity(urn, out var json) ? Encoding.UTF8.GetString(json.Span) : null;
        }

        private static JsonElement Aspects(DataHubLineageStore store, string urn)
        {
            return JsonDocument.Parse(Text(store, urn)!).RootElement.GetProperty("aspects");
        }

        private static JsonElement RunEvent(DataHubLineageStore store, string urn)
        {
            var runEvent = Aspects(store, urn).GetProperty("dataProcessInstanceRunEvent");
            Assert.Equal("TIMESERIES", runEvent.GetProperty("type").GetString());
            return runEvent.GetProperty("value");
        }

        private static string Status(DataHubLineageStore store, string urn)
        {
            var runEvent = RunEvent(store, urn);
            var status = runEvent.GetProperty("status").GetString()!;
            return runEvent.TryGetProperty("result", out var result) ? status + ":" + result.GetProperty("type").GetString() : status;
        }

        // Single quotes keep the golden JSON readable.
        private static string Json(string text)
        {
            return text.Replace('\'', '"');
        }

        [Fact]
        public void StartedStreamIsServedAsARunningRun()
        {
            var store = Store(new ManualClock());
            var generation = store.Register(Lineage(), "a");
            var urn = RunUrn("a");
            var start = Start.ToUnixTimeMilliseconds();
            Assert.Contains(urn, store.GetSnapshot().Urns);
            Assert.Null(Text(store, urn));

            store.RecordStreamState(generation, StreamStateValue.Starting);

            var aspects = Aspects(store, urn);
            Assert.Equal(
                ["dataProcessInstanceProperties", "dataProcessInstanceRelationships", "dataProcessInstanceInput", "dataProcessInstanceOutput", "dataPlatformInstance", "dataProcessInstanceRunEvent"],
                aspects.EnumerateObject().Select(x => x.Name));
            Assert.Equal(Json($"{{'customProperties':{{'flowtide.stream':'a'}},'name':'a','type':'STREAMING','created':{{'time':{start},'actor':'urn:li:corpuser:flowtide'}}}}"), aspects.GetProperty("dataProcessInstanceProperties").GetProperty("value").GetRawText());
            Assert.Equal(Json($"{{'parentTemplate':'{Job}','upstreamInstances':[]}}"), aspects.GetProperty("dataProcessInstanceRelationships").GetProperty("value").GetRawText());
            Assert.Equal(Json($"{{'inputs':['{DatasetS}']}}"), aspects.GetProperty("dataProcessInstanceInput").GetProperty("value").GetRawText());
            Assert.Equal(Json($"{{'outputs':['{DatasetT}']}}"), aspects.GetProperty("dataProcessInstanceOutput").GetProperty("value").GetRawText());
            Assert.Equal(Json("{'platform':'urn:li:dataPlatform:flowtide'}"), aspects.GetProperty("dataPlatformInstance").GetProperty("value").GetRawText());
            Assert.Equal(Json($"{{'timestampMillis':{start},'status':'STARTED'}}"), RunEvent(store, urn).GetRawText());
        }

        [Fact]
        public void RunFollowsFailuresRestartsAndAStop()
        {
            var clock = new ManualClock();
            var store = Store(clock);
            var generation = store.Register(Lineage(), "a");
            var urn = RunUrn("a");
            var start = Start.ToUnixTimeMilliseconds();
            store.RecordStreamState(generation, StreamStateValue.Starting);
            var started = Text(store, urn);

            // Running keeps the bytes, so DataHub writes nothing new.
            clock.Now = Start.AddSeconds(5);
            store.RecordStreamState(generation, StreamStateValue.Running);
            Assert.Equal(started, Text(store, urn));

            clock.Now = Start.AddMinutes(1);
            store.RecordStreamState(generation, StreamStateValue.Failure);
            Assert.Equal(Json($"{{'timestampMillis':{start + 60_000},'status':'COMPLETE','result':{{'type':'FAILURE','nativeResultType':'flowtide'}},'durationMillis':60000}}"), RunEvent(store, urn).GetRawText());

            clock.Now = Start.AddMinutes(2);
            store.RecordStreamState(generation, StreamStateValue.Starting);
            store.RecordStreamState(generation, StreamStateValue.Running);
            Assert.Equal(("STARTED", start + 120_000), (Status(store, urn), RunEvent(store, urn).GetProperty("timestampMillis").GetInt64()));
            // The run's time stays at its first start in this process.
            Assert.Equal(start, Aspects(store, urn).GetProperty("dataProcessInstanceProperties").GetProperty("value").GetProperty("created").GetProperty("time").GetInt64());

            clock.Now = Start.AddMinutes(3);
            store.RecordStreamState(generation, StreamStateValue.Stopping);
            Assert.Equal("STARTED", Status(store, urn));
            store.RecordStreamState(generation, StreamStateValue.NotStarted);
            Assert.Equal(("COMPLETE:SUCCESS", 60_000L), (Status(store, urn), RunEvent(store, urn).GetProperty("durationMillis").GetInt64()));
        }

        [Fact]
        public void StopAfterAFailureKeepsTheFailure()
        {
            var store = Store(new ManualClock());
            var generation = store.Register(Lineage(), "a");
            var urn = RunUrn("a");
            store.RecordStreamState(generation, StreamStateValue.Starting);
            store.RecordStreamState(generation, StreamStateValue.Failure);

            store.RecordStreamState(generation, StreamStateValue.NotStarted);

            Assert.Equal("COMPLETE:FAILURE", Status(store, urn));
        }

        [Fact]
        public void CancelledStartEndsByTheStopOrDelete()
        {
            var store = Store(new ManualClock());
            var stopped = store.Register(Lineage("stopped"), "a");
            var deleted = store.Register(Lineage("deleted"), "a");
            foreach (var (generation, substream) in new[] { (stopped, "stopped"), (deleted, "deleted") })
            {
                store.RecordStreamState(generation, StreamStateValue.Starting);
                store.RecordStreamFailure(generation, cancellation: true);
                store.RecordStreamState(generation, StreamStateValue.Failure);
                Assert.Equal("STARTED", Status(store, RunUrn("a", substream)));
            }

            store.RecordStreamState(stopped, StreamStateValue.NotStarted);
            store.RecordStreamState(deleted, StreamStateValue.Deleting);
            store.RecordStreamState(deleted, StreamStateValue.Deleted);

            Assert.Equal(("COMPLETE:SUCCESS", "COMPLETE:SKIPPED"), (Status(store, RunUrn("a", "stopped")), Status(store, RunUrn("a", "deleted"))));
        }

        private static void Notify(FlowtideDotNet.Lineage.DataHub.Internal.DataHubLineageListener listener, StreamStateValue state)
        {
            var name = "a";
            listener.OnStreamStateChange(new StreamStateChangeNotification(ref name, ref state));
        }

        private static void NotifyFailure(FlowtideDotNet.Lineage.DataHub.Internal.DataHubLineageListener listener, Exception exception)
        {
            var name = "a";
            listener.OnFailure(new StreamFailureNotification(ref name, exception));
        }

        // Only the engine's own stop or delete during the start is a cancellation, timeouts are failures.
        [Theory]
        [InlineData("timeout")]
        [InlineData("checkpoint")]
        [InlineData("handshake")]
        public void CancellationExceptionsThatAreNotAStopAreFailures(string kind)
        {
            var store = Store(new ManualClock());
            var listener = new FlowtideDotNet.Lineage.DataHub.Internal.DataHubLineageListener(store);
            listener.OnStreamBuilt(Lineage(), "a");
            Exception exception = kind switch
            {
                "timeout" => new TaskCanceledException("A task was canceled."),
                "checkpoint" => new OperationCanceledException("The stop timed out waiting for a checkpoint to complete."),
                _ => new OperationCanceledException("The handshake was superseded.")
            };
            Notify(listener, StreamStateValue.Starting);
            Notify(listener, StreamStateValue.Running);

            NotifyFailure(listener, exception);
            Notify(listener, StreamStateValue.Failure);
            Assert.Equal("COMPLETE:FAILURE", Status(store, RunUrn("a")));

            // A stop while it backs off keeps the failure.
            Notify(listener, StreamStateValue.NotStarted);
            Assert.Equal("COMPLETE:FAILURE", Status(store, RunUrn("a")));
        }

        [Theory]
        [InlineData("The stream was stopped while it was starting.", StreamStateValue.NotStarted, "COMPLETE:SUCCESS")]
        [InlineData("The stream was deleted while it was starting.", StreamStateValue.Deleted, "COMPLETE:SKIPPED")]
        public void EngineCancellationOfTheStartIsNotAFailure(string message, StreamStateValue end, string expected)
        {
            var store = Store(new ManualClock());
            var listener = new FlowtideDotNet.Lineage.DataHub.Internal.DataHubLineageListener(store);
            listener.OnStreamBuilt(Lineage(), "a");
            Notify(listener, StreamStateValue.Starting);

            NotifyFailure(listener, new OperationCanceledException(message));
            Notify(listener, StreamStateValue.Failure);
            Assert.Equal("STARTED", Status(store, RunUrn("a")));
            if (end == StreamStateValue.Deleted)
            {
                Notify(listener, StreamStateValue.Deleting);
            }
            Notify(listener, end);

            Assert.Equal(expected, Status(store, RunUrn("a")));
        }

        [Fact]
        public void FailureAfterACancelledStartIsAFailure()
        {
            var store = Store(new ManualClock());
            var listener = new FlowtideDotNet.Lineage.DataHub.Internal.DataHubLineageListener(store);
            listener.OnStreamBuilt(Lineage(), "a");
            Notify(listener, StreamStateValue.Starting);
            NotifyFailure(listener, new OperationCanceledException("The stream was stopped while it was starting."));
            Notify(listener, StreamStateValue.Failure);

            // A failure while already failing brings no new state change.
            NotifyFailure(listener, new InvalidOperationException("source failed"));
            Assert.Equal("COMPLETE:FAILURE", Status(store, RunUrn("a")));
            Notify(listener, StreamStateValue.NotStarted);

            Assert.Equal("COMPLETE:FAILURE", Status(store, RunUrn("a")));
        }

        [Fact]
        public void StopThatReportsAFailureEndsAsFailed()
        {
            var store = Store(new ManualClock());
            var generation = store.Register(Lineage(), "a");
            store.RecordStreamState(generation, StreamStateValue.Starting);
            store.RecordStreamState(generation, StreamStateValue.Running);
            store.RecordStreamState(generation, StreamStateValue.Stopping);

            store.RecordStreamFailure(generation, cancellation: false);
            store.RecordStreamState(generation, StreamStateValue.NotStarted);

            Assert.Equal("COMPLETE:FAILURE", Status(store, RunUrn("a")));
        }

        [Fact]
        public async Task StopDuringTheStartEndsAsSucceeded()
        {
            var source = new ControlledSource() { BlockInit = true };
            var (provider, store, stream) = BuildStream("stopstart", source);
            await using var _ = provider;
            var start = Task.Run(() => stream.StartAsync());
            await source.InitEntered.Task.WaitAsync(TimeSpan.FromSeconds(60));

            var stop = stream.StopAsync();
            await WaitFor(() => stream.State == StreamStateValue.Failure);
            source.InitGate.TrySetResult();
            await stop.WaitAsync(TimeSpan.FromSeconds(60));
            await WaitFor(() => stream.State == StreamStateValue.NotStarted);

            Assert.Equal("COMPLETE:SUCCESS", Status(store, RunUrn("stopstart", null, "blackhole.output")));
        }

        [Fact]
        public async Task DeleteDuringTheStartEndsAsCancelled()
        {
            var source = new ControlledSource() { BlockInit = true };
            var (provider, store, stream) = BuildStream("deletestart", source);
            await using var _ = provider;
            var start = Task.Run(() => stream.StartAsync());
            await source.InitEntered.Task.WaitAsync(TimeSpan.FromSeconds(60));

            var delete = stream.DeleteAsync();
            await WaitFor(() => stream.State == StreamStateValue.Failure);
            source.InitGate.TrySetResult();
            await delete.WaitAsync(TimeSpan.FromSeconds(60));

            Assert.Equal("COMPLETE:SKIPPED", Status(store, RunUrn("deletestart", null, "blackhole.output")));
        }

        [Fact]
        public async Task StopWithAFailingCheckpointEndsAsFailed()
        {
            var source = new ControlledSource();
            var (provider, store, stream) = BuildStream("failedstop", source);
            await using var _ = provider;
            await stream.StartAsync();
            await WaitFor(() => stream.State == StreamStateValue.Running);
            // The first checkpoint must pass.
            await Task.Delay(2000);
            source.FailCheckpoints = true;

            try
            {
                await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(90));
            }
            catch (Exception)
            {
            }
            await WaitFor(() => stream.State == StreamStateValue.NotStarted);

            Assert.Equal("COMPLETE:FAILURE", Status(store, RunUrn("failedstop", null, "blackhole.output")));
        }

        [Fact]
        public void DeletedStreamEndsTheRunAsCancelled()
        {
            var store = Store(new ManualClock());
            var generation = store.Register(Lineage(), "a");
            var urn = RunUrn("a");
            store.RecordStreamState(generation, StreamStateValue.Starting);
            store.RecordStreamState(generation, StreamStateValue.Running);

            store.RecordStreamState(generation, StreamStateValue.Deleting);
            store.RecordStreamState(generation, StreamStateValue.Deleted);

            Assert.Equal("COMPLETE:SKIPPED", Status(store, urn));
        }

        [Fact]
        public void EverySubstreamHasItsOwnRun()
        {
            var store = Store(new ManualClock());
            var sub0 = store.Register(Lineage("sub0"), "a");
            var sub1 = store.Register(Lineage("sub1"), "a");
            store.RecordStreamState(sub0, StreamStateValue.Starting);
            store.RecordStreamState(sub1, StreamStateValue.Starting);
            store.RecordStreamState(sub1, StreamStateValue.Failure);

            Assert.Equal(new[] { RunUrn("a", "sub0"), RunUrn("a", "sub1") }.Order(StringComparer.Ordinal), RunUrns(store));
            var properties = Aspects(store, RunUrn("a", "sub1")).GetProperty("dataProcessInstanceProperties").GetProperty("value");
            Assert.Equal(("a/sub1", "sub1"), (properties.GetProperty("name").GetString(), properties.GetProperty("customProperties").GetProperty("flowtide.substream").GetString()));
            Assert.Equal(Job, Aspects(store, RunUrn("a", "sub1")).GetProperty("dataProcessInstanceRelationships").GetProperty("value").GetProperty("parentTemplate").GetString());
            Assert.Equal(("STARTED", "COMPLETE:FAILURE"), (Status(store, RunUrn("a", "sub0")), Status(store, RunUrn("a", "sub1"))));
        }

        [Fact]
        public void RebuildTakesTheRunOverOnceItStarts()
        {
            var clock = new ManualClock();
            var store = Store(clock);
            var first = store.Register(Lineage(), "a");
            var urn = RunUrn("a");
            store.RecordStreamState(first, StreamStateValue.Starting);

            clock.Now = Start.AddMinutes(1);
            var rebuilt = store.Register(Lineage(), "a");
            // The replaced build reports nothing any more.
            store.RecordStreamState(first, StreamStateValue.Failure);
            Assert.Null(Text(store, urn));

            store.RecordStreamState(rebuilt, StreamStateValue.Starting);

            Assert.Equal(("STARTED", Start.AddMinutes(1).ToUnixTimeMilliseconds()), (Status(store, urn), RunEvent(store, urn).GetProperty("timestampMillis").GetInt64()));
            Assert.Equal(Start.AddMinutes(1).ToUnixTimeMilliseconds(), Aspects(store, urn).GetProperty("dataProcessInstanceProperties").GetProperty("value").GetProperty("created").GetProperty("time").GetInt64());
        }

        [Fact]
        public void EveryJobHasARunWithItsOwnInputs()
        {
            var store = Store(new ManualClock());
            var generation = store.Register(Snapshot(
                [Input("postgres", "s", [Col("x", new Int64Type())]), Input("postgres", "v", [Col("y", new Int64Type())])],
                [
                    Output("postgres", "t", [Col("x", new Int64Type())], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"]),
                    Output("postgres", "u", [Col("y", new Int64Type())], new() { ["y"] = [Identity("postgres", "v", "y")] }, upstream: ["v"])
                ]), "a");
            store.RecordStreamState(generation, StreamStateValue.Starting);
            var runT = RunUrn("a");
            var runU = RunUrn("a", null, "postgres.db.public.u");

            Assert.Equal(new[] { runT, runU }.Order(StringComparer.Ordinal), RunUrns(store));
            var u = Aspects(store, runU);
            Assert.Equal("urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),postgres.db.public.u)", u.GetProperty("dataProcessInstanceRelationships").GetProperty("value").GetProperty("parentTemplate").GetString());
            Assert.Equal(["urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.v,PROD)"], u.GetProperty("dataProcessInstanceInput").GetProperty("value").GetProperty("inputs").EnumerateArray().Select(x => x.GetString()));
            Assert.Equal(["urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.u,PROD)"], u.GetProperty("dataProcessInstanceOutput").GetProperty("value").GetProperty("outputs").EnumerateArray().Select(x => x.GetString()));
            Assert.Equal(("STARTED", "STARTED"), (Status(store, runT), Status(store, runU)));
        }

        private static string FlowRunUrn(string stream, string? substream = null)
        {
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"flowtide\u001furn:li:dataFlow:(flowtide,{stream},PROD)\u001f{substream}"));
            return "urn:li:dataProcessInstance:" + Convert.ToHexString(hash, 0, 16).ToLowerInvariant();
        }

        // Its failure would otherwise be invisible while its peers wait for it in Starting.
        [Fact]
        public void SubstreamThatWritesNoTableHasARunOnTheFlow()
        {
            var store = Store(new ManualClock());
            var sub0 = store.Register(Lineage("sub0"), "a");
            var sub1 = store.Register(Snapshot([Input("postgres", "s", [Col("x", new Int64Type())])], [], "sub1"), "a");
            store.RecordStreamState(sub0, StreamStateValue.Starting);
            store.RecordStreamState(sub1, StreamStateValue.Starting);

            store.RecordStreamState(sub1, StreamStateValue.Failure);

            Assert.Equal(new[] { RunUrn("a", "sub0"), FlowRunUrn("a", "sub1") }.Order(StringComparer.Ordinal), RunUrns(store));
            var run = Aspects(store, FlowRunUrn("a", "sub1"));
            Assert.Equal("urn:li:dataFlow:(flowtide,a,PROD)", run.GetProperty("dataProcessInstanceRelationships").GetProperty("value").GetProperty("parentTemplate").GetString());
            Assert.Equal([DatasetS], run.GetProperty("dataProcessInstanceInput").GetProperty("value").GetProperty("inputs").EnumerateArray().Select(x => x.GetString()));
            Assert.Empty(run.GetProperty("dataProcessInstanceOutput").GetProperty("value").GetProperty("outputs").EnumerateArray());
            Assert.Equal(("STARTED", "COMPLETE:FAILURE"), (Status(store, RunUrn("a", "sub0")), Status(store, FlowRunUrn("a", "sub1"))));
        }

        // Peers register in any order and move between processes, a run must not move with them.
        [Fact]
        public void RunOfASubstreamThatWritesNoTableKeepsItsUrnWhenPeersRegister()
        {
            var store = Store(new ManualClock());
            store.Register(Snapshot([Input("postgres", "s", [Col("x", new Int64Type())])], [], "sub1"), "a");
            var alone = RunUrns(store);

            store.Register(Lineage("sub0"), "a");

            Assert.Equal([FlowRunUrn("a", "sub1")], alone);
            Assert.Contains(FlowRunUrn("a", "sub1"), RunUrns(store));
        }

        [Fact]
        public void StreamWithoutJobsHasARunOnItsFlow()
        {
            var store = Store(new ManualClock());
            var generation = store.Register(Snapshot([Input("postgres", "s", [Col("x", new Int64Type())])], []), "a");
            store.RecordStreamState(generation, StreamStateValue.Starting);
            store.RecordStreamState(generation, StreamStateValue.Failure);
            var flow = "urn:li:dataFlow:(flowtide,a,PROD)";
            var urn = "urn:li:dataProcessInstance:" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes($"flowtide\u001f{flow}\u001f")), 0, 16).ToLowerInvariant();

            Assert.Equal([urn], RunUrns(store));
            Assert.Equal(flow, Aspects(store, urn).GetProperty("dataProcessInstanceRelationships").GetProperty("value").GetProperty("parentTemplate").GetString());
            Assert.Equal("COMPLETE:FAILURE", Status(store, urn));
        }

        [Fact]
        public void RunsCanBeTurnedOff()
        {
            var store = Store(new ManualClock(), o => o.IncludeRuns = false);
            var generation = store.Register(Lineage(), "a");

            store.RecordStreamState(generation, StreamStateValue.Starting);

            Assert.DoesNotContain(store.GetSnapshot().Urns, x => x.StartsWith("urn:li:dataProcessInstance:", StringComparison.Ordinal));
        }

        [Fact]
        public async Task RunningStreamIsARunThatEndsOnStop()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddFlowtideStream("runs")
                .AddSqlTextAsPlan("INSERT INTO output SELECT Key FROM rows")
                .AddConnectors(c =>
                {
                    c.AddCustomSource("rows", _ => new RowSource());
                    c.AddBlackholeSink("*");
                })
                .AddStorage(s => s.AddTemporaryDevelopmentStorage(o => o.DirectoryPath = $"./data/tempFiles/datahubruns{Guid.NewGuid():N}"))
                .AddDataHubLineage();
            await using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();
            var stream = provider.GetRequiredKeyedService<FlowtideDotNet.Base.Engine.DataflowStream>("runs");
            var urn = RunUrn("runs", null, "blackhole.output");
            Assert.Null(Text(store, urn));

            await stream.StartAsync();
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(60) && stream.State != StreamStateValue.Running)
            {
                await Task.Delay(10);
            }
            Assert.Equal("STARTED", Status(store, urn));
            Assert.Equal(["urn:li:dataset:(urn:li:dataPlatform:custom_source,rows,PROD)"], Aspects(store, urn).GetProperty("dataProcessInstanceInput").GetProperty("value").GetProperty("inputs").EnumerateArray().Select(x => x.GetString()));

            await stream.StopAsync();

            Assert.Equal("COMPLETE:SUCCESS", Status(store, urn));
        }
    }
}
