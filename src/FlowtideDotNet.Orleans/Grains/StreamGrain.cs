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

using FlowtideDotNet.Core;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Grains
{
    [GenerateSerializer]
    public class StreamGrainStorage
    {
        [Id(0)]
        public List<string> StartedSubstreams { get; set; } = new List<string>();

        /// <summary>
        /// The SQL text the stream was started with, used to reject a start with a changed
        /// plan while the stream is running. Null when the stream was started from a plan.
        /// </summary>
        [Id(1)]
        public string? SqlText { get; set; }

        [Id(2)]
        public int? SubstreamCount { get; set; }

        /// <summary>
        /// The user provided plan the stream was started with, as serialized Substrait JSON.
        /// Null when the stream was started from SQL text.
        /// </summary>
        [Id(3)]
        public string? PlanJson { get; set; }

        [Id(4)]
        public bool OptimizePlan { get; set; }
    }

    internal class StreamGrain : Grain, IStreamGrain
    {
        private readonly ConnectorManagerFactory connectorManagerFactory;
        private readonly IPersistentState<StreamGrainStorage> _state;
        private readonly ILogger<StreamGrain> _logger;

        public StreamGrain(
            ConnectorManagerFactory connectorManagerFactory,
            [PersistentState("stream", "stream_metadata")] IPersistentState<StreamGrainStorage> state,
            ILogger<StreamGrain> logger)
        {
            this.connectorManagerFactory = connectorManagerFactory;
            _state = state;
            _logger = logger;
        }

        public async Task StartStreamAsync(StartStreamRequest request)
        {
            if (request.SqlText == null && request.PlanJson == null)
            {
                throw new ArgumentException("Either SqlText or PlanJson must be set on the start request.", nameof(request));
            }

            // A started stream keeps the plan it was started with, silently accepting a
            // changed plan would mix substreams running the old and the new plan.
            // Starting again with the identical request is a no-op retry.
            if (_state.RecordExists && (_state.State.SqlText != null || _state.State.PlanJson != null))
            {
                if (!string.Equals(_state.State.SqlText, request.SqlText, StringComparison.Ordinal) ||
                    !string.Equals(_state.State.PlanJson, request.PlanJson, StringComparison.Ordinal) ||
                    _state.State.SubstreamCount != request.SubstreamCount ||
                    _state.State.OptimizePlan != request.OptimizePlan)
                {
                    throw new InvalidOperationException(
                        $"Stream '{this.GetPrimaryKeyString()}' is already started with a different plan or substream count. Stop the stream before starting it with a new plan.");
                }
            }

            // The plan is prepared once, centrally: compiled from SQL with the silo's
            // connectors, or deserialized from a user provided plan. The substream grains get
            // the final distributed plan serialized, they never build plans themselves.
            var plan = BuildPlan(request);
            var planJson = SubstraitSerializer.SerializeToJson(plan);
            var substreams = GetSubstreamNames(plan);

            // The started set is persisted before the substream grains start, a stop uses
            // only this set. A missed substream grain would keep running forever, restarted
            // by its keep alive reminder with no way left to stop it.
            _state.State.SqlText = request.SqlText;
            _state.State.PlanJson = request.PlanJson;
            _state.State.SubstreamCount = request.SubstreamCount;
            _state.State.OptimizePlan = request.OptimizePlan;
            foreach (var substream in substreams)
            {
                if (!_state.State.StartedSubstreams.Contains(substream))
                {
                    _state.State.StartedSubstreams.Add(substream);
                }
            }
            await _state.WriteStateAsync();

            List<Task> startTasks = new List<Task>();
            foreach (var substream in substreams)
            {
                var substreamKey = SubStreamGrainKey.Create(this.GetPrimaryKeyString(), substream);
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                startTasks.Add(substreamGrain.StartStreamAsync(new StartStreamMessage(this.GetPrimaryKeyString(), planJson, substream)));
            }

            await Task.WhenAll(startTasks);
        }

        private Plan BuildPlan(StartStreamRequest request)
        {
            if (request.PlanJson != null)
            {
                var plan = new SubstraitDeserializer().Deserialize(request.PlanJson);
                if (request.OptimizePlan)
                {
                    return OrleansStreamPlanBuilder.OptimizePlan(plan, request.SubstreamCount);
                }
                // The user optimized (and possibly distributed) the plan themselves, it runs
                // exactly as given. The distributor throws on an already distributed plan, so
                // pre-distributed plans must come through this path.
                return plan;
            }
            return OrleansStreamPlanBuilder.BuildPlan(connectorManagerFactory.Create(this.GetPrimaryKeyString()), request.SqlText!, request.SubstreamCount);
        }

        public async Task StopStreamAsync()
        {
            // All substreams are stopped together so the coordinated stop can drain the data
            // exchanged between them, a substream only finishes stopping when the other
            // substreams have received everything it sent before its stop barrier.
            List<Task> stopTasks = new List<Task>();
            foreach (var substream in _state.State.StartedSubstreams)
            {
                var substreamKey = SubStreamGrainKey.Create(this.GetPrimaryKeyString(), substream);
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                stopTasks.Add(CallWithTimeoutRetry(() => substreamGrain.StopStreamAsync(), "stop", substream));
            }

            await Task.WhenAll(stopTasks);

            _state.State.StartedSubstreams.Clear();
            await _state.ClearStateAsync();
        }

        public async Task DeleteStreamAsync()
        {
            // Deleted in parallel like the coordinated stop, deleting one substream while
            // its peers keep running would have the peers recover against missing state.
            List<Task> deleteTasks = new List<Task>();
            foreach (var substream in _state.State.StartedSubstreams)
            {
                var substreamKey = SubStreamGrainKey.Create(this.GetPrimaryKeyString(), substream);
                var substreamGrain = GrainFactory.GetGrain<ISubStreamGrain>(substreamKey);
                deleteTasks.Add(CallWithTimeoutRetry(() => substreamGrain.DeleteStreamAsync(), "delete", substream));
            }

            await Task.WhenAll(deleteTasks);

            _state.State.StartedSubstreams.Clear();
            await _state.ClearStateAsync();
        }

        /// <summary>
        /// A stop or delete can legitimately execute longer than one grain response timeout,
        /// for example while a slow teardown drains under load. Both calls are idempotent on
        /// the substream grain - a repeated call joins the teardown already in progress - so
        /// the coordination retries a timed out call instead of failing the whole operation
        /// while the substream is still working on it.
        /// </summary>
        private async Task CallWithTimeoutRetry(Func<Task> call, string operation, string substream)
        {
            // Bounded low: this grain is not reentrant, so time spent retrying here keeps the
            // callers own retry queued behind it. The retries only need to cover a substream
            // teardown that runs slightly past one response timeout, a caller with a larger
            // budget retries the whole idempotent call.
            const int maxAttempts = 3;
            for (int attempt = 1; ; attempt++)
            {
                try
                {
                    await call();
                    return;
                }
                catch (TimeoutException) when (attempt < maxAttempts)
                {
                    _logger.LogWarning(
                        "The {operation} call to substream {substream} timed out, the teardown may still be in progress, retrying (attempt {attempt}).",
                        operation, substream, attempt);
                }
            }
        }

        public async Task<StreamStatusResponse> GetStatusAsync()
        {
            var response = new StreamStatusResponse
            {
                IsStarted = _state.RecordExists
            };
            var statusTasks = _state.State.StartedSubstreams.Select(async substream =>
            {
                var substreamKey = SubStreamGrainKey.Create(this.GetPrimaryKeyString(), substream);
                try
                {
                    var status = await GrainFactory.GetGrain<ISubStreamGrain>(substreamKey).GetStatusAsync();
                    // A substream grain that lost its state does not know its own name
                    status.SubstreamName = substream;
                    return status;
                }
                catch (Exception e)
                {
                    // An unreachable substream grain must not fail the whole status call
                    return new SubstreamStatus
                    {
                        SubstreamName = substream,
                        Error = e.Message
                    };
                }
            }).ToList();
            response.Substreams.AddRange(await Task.WhenAll(statusTasks));
            return response;
        }

        private static HashSet<string> GetSubstreamNames(Plan plan)
        {
            HashSet<string> substreams = new HashSet<string>();

            // Find out the different substreams
            foreach(var relation in plan.Relations)
            {
                if (relation is SubStreamRootRelation substreamRoot)
                {
                    substreams.Add(substreamRoot.Name);
                }
            }
            if (substreams.Count == 0)
            {
                substreams.Add("default");
            }
            return substreams;
        }
    }
}
