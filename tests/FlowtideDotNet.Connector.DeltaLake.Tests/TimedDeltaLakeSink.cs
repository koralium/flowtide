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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Times the sink's checkpoints and publications, for probes
    internal sealed class TimedDeltaLakeSink : DeltaLakeSink
    {
        private readonly Action<TimeSpan, TimeSpan> _timed;
        private TimeSpan _checkpoint;

        public TimedDeltaLakeSink(DeltaLakeOptions options, WriteRelation writeRelation, ExecutionDataflowBlockOptions blockOptions, Action<TimeSpan, TimeSpan> timed)
            : base(options, writeRelation, blockOptions)
        {
            _timed = timed;
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            var started = Stopwatch.GetTimestamp();
            await base.OnCheckpoint(checkpointTime);
            _checkpoint = Stopwatch.GetElapsedTime(started);
        }

        // Called after each checkpoint, with that checkpoint's version
        public override async Task CommitVersion(long version)
        {
            var started = Stopwatch.GetTimestamp();
            await base.CommitVersion(version);
            _timed(_checkpoint, Stopwatch.GetElapsedTime(started));
        }
    }

    internal sealed class TimedSinkFactory : IConnectorSinkFactory
    {
        private readonly DeltaLakeOptions _options;
        private readonly Action<TimeSpan, TimeSpan> _timed;

        public TimedSinkFactory(DeltaLakeOptions options, Action<TimeSpan, TimeSpan> timed)
        {
            _options = options;
            _timed = timed;
        }

        public bool CanHandle(WriteRelation writeRelation)
        {
            return true;
        }

        public Relation ModifyPlan(WriteRelation writeRelation)
        {
            return writeRelation;
        }

        public IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
        {
            return new TimedDeltaLakeSink(_options, writeRelation, dataflowBlockOptions, _timed);
        }

        public TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
        {
            return new TableLineageMetadata("delta_table", writeRelation.NamedObject.DotSeperated, writeRelation.TableSchema);
        }
    }
}
