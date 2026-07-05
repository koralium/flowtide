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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Core.Engine.Distributed
{
    /// <summary>
    /// Builds a distributed stream where all substreams run in the same process.
    ///
    /// The plan can either already contain substreams (for example from SQL SUBSTREAM statements)
    /// or be split automatically with <see cref="DistributeAutomatically"/>.
    /// Each substream runs as its own stream with its own state storage, the substreams
    /// communicate in process through a <see cref="LocalSubstreamCommunicationHub"/>.
    /// </summary>
    public class DistributedStreamBuilder
    {
        private readonly string _streamName;
        private Func<Plan>? _planFactory;
        private Func<string, IConnectorManager>? _connectorManagerFactory;
        private Func<string, StateManagerOptions>? _stateOptionsFactory;
        private ILoggerFactory? _loggerFactory;
        private PlanOptimizerSettings? _planOptimizerSettings;
        private bool _optimizePlan = true;
        private DistributedPlanOptions? _distributedPlanOptions;
        private Action<string, FlowtideBuilder>? _configureSubstream;

        public DistributedStreamBuilder(string streamName)
        {
            _streamName = streamName;
        }

        /// <summary>
        /// Adds a factory that creates the plan to run.
        ///
        /// The factory is called once per substream since building a stream modifies the plan
        /// in place, each substream requires its own plan instance.
        /// The factory must be deterministic and return an identical plan on every call,
        /// for example by rebuilding it from the same SQL text, otherwise the substreams
        /// cannot communicate with each other.
        /// </summary>
        /// <param name="planFactory">Factory that creates a new identical plan on every call.</param>
        /// <param name="optimize">
        /// If true each plan is optimized before it is handed to the substream, the optimizer is
        /// deterministic so all substreams get the same result. When false together with
        /// <see cref="DistributeAutomatically"/> the factory must return an already optimized plan,
        /// distribution can only partition joins that have been converted to merge joins.
        /// </param>
        /// <param name="planOptimizerSettings">Optional optimizer settings.</param>
        public DistributedStreamBuilder AddPlan(Func<Plan> planFactory, bool optimize = true, PlanOptimizerSettings? planOptimizerSettings = default)
        {
            _planFactory = planFactory;
            _optimizePlan = optimize;
            _planOptimizerSettings = planOptimizerSettings;
            return this;
        }

        /// <summary>
        /// Sets the factory that creates the connector manager for each substream, called once
        /// per substream with the substream name. Each substream gets its own manager so
        /// connector factories that hold per stream state, for example sinks capturing
        /// callbacks, are never shared between substreams.
        /// </summary>
        public DistributedStreamBuilder AddConnectorManager(Func<string, IConnectorManager> connectorManagerFactory)
        {
            _connectorManagerFactory = connectorManagerFactory;
            return this;
        }

        /// <summary>
        /// Sets the factory that creates the state manager options for each substream.
        /// Each substream must have its own storage location, the factory gets the substream
        /// name as input.
        /// </summary>
        public DistributedStreamBuilder WithStateOptionsFactory(Func<string, StateManagerOptions> stateOptionsFactory)
        {
            _stateOptionsFactory = stateOptionsFactory;
            return this;
        }

        public DistributedStreamBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            return this;
        }

        /// <summary>
        /// Splits a plan that does not yet contain substreams into the given number of substreams
        /// using <see cref="DistributedPlanModifier"/>.
        /// </summary>
        public DistributedStreamBuilder DistributeAutomatically(int substreamCount, Func<int, string>? substreamNameGenerator = default)
        {
            if (substreamCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(substreamCount), substreamCount, "The substream count must be at least 1.");
            }
            _distributedPlanOptions = new DistributedPlanOptions()
            {
                SubstreamCount = substreamCount,
                SubstreamNameGenerator = substreamNameGenerator
            };
            return this;
        }

        /// <summary>
        /// Optional hook to configure each substream builder, called with the substream name.
        /// Can be used to set listeners, schedulers or override any other setting per substream.
        /// </summary>
        public DistributedStreamBuilder ConfigureSubstream(Action<string, FlowtideBuilder> configureSubstream)
        {
            _configureSubstream = configureSubstream;
            return this;
        }

        public DistributedFlowtideStream Build()
        {
            if (_planFactory == null)
            {
                throw new InvalidOperationException("A plan must be added before building the distributed stream.");
            }
            if (_stateOptionsFactory == null)
            {
                throw new InvalidOperationException("A state options factory must be set before building the distributed stream, each substream requires its own storage.");
            }

            // Find all substreams in the plan
            var substreamNames = new List<string>();
            foreach (var relation in CreatePlan().Relations)
            {
                if (relation is SubStreamRootRelation subStreamRootRelation &&
                    !substreamNames.Contains(subStreamRootRelation.Name))
                {
                    substreamNames.Add(subStreamRootRelation.Name);
                }
            }
            if (substreamNames.Count == 0)
            {
                // No substreams in the plan, run it as a single substream
                substreamNames.Add("default");
            }

            var hub = new LocalSubstreamCommunicationHub();
            var substreams = new Dictionary<string, Base.Engine.DataflowStream>();
            foreach (var substreamName in substreamNames)
            {
                var builder = new FlowtideBuilder($"{_streamName}_{substreamName}")
                    .AddPlan(CreatePlan(), false)
                    .WithStateOptions(_stateOptionsFactory(substreamName));

                if (_connectorManagerFactory != null)
                {
                    builder.AddConnectorManager(_connectorManagerFactory(substreamName));
                }
                if (_loggerFactory != null)
                {
                    builder.WithLoggerFactory(_loggerFactory);
                }

                builder.SetDistributedOptions(new DistributedOptions(
                    substreamName,
                    default,
                    hub.CreateFactory(substreamName)));

                _configureSubstream?.Invoke(substreamName, builder);

                substreams.Add(substreamName, builder.Build());
            }

            return new DistributedFlowtideStream(_streamName, substreams, _loggerFactory?.CreateLogger<DistributedFlowtideStream>());
        }

        /// <summary>
        /// Creates the plan for one substream.
        /// Building a stream modifies the plan in place, so each substream must get its own
        /// plan instance. The plan factory, the distributed plan modifier and the optimizer
        /// are all deterministic which makes every instance identical.
        /// </summary>
        private Plan CreatePlan()
        {
            var plan = _planFactory!();

            if (_optimizePlan)
            {
                // Distribution runs as the last step inside the optimizer, after joins have
                // been converted to merge joins so they can be partitioned across substreams.
                var settings = _planOptimizerSettings ?? new PlanOptimizerSettings();
                if (_distributedPlanOptions != null)
                {
                    settings.DistributedPlanOptions = _distributedPlanOptions;
                }
                plan = PlanOptimizer.Optimize(plan, settings);
            }
            else if (_distributedPlanOptions != null)
            {
                // The plan is already optimized by the caller, distribute it directly
                plan = DistributedPlanModifier.ToDistributedPlan(plan, _distributedPlanOptions);
            }
            return plan;
        }
    }
}
