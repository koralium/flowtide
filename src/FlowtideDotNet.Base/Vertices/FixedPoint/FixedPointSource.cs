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

using FlowtideDotNet.Base.dataflow;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.FixedPoint
{
    /// <summary>
    /// Represents a source for output from the fixed point computation.
    /// This is a seperate class to handle linking between crashes.
    /// </summary>
    internal class FixedPointSource : ISourceBlock<IStreamEvent>, IStreamVertex
    {
        private TransformBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>? _block;
        private ISourceBlock<IStreamEvent>? _source;
        private readonly List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)> _links = new List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)>();
        private readonly ExecutionDataflowBlockOptions executionDataflowBlockOptions;
        private string? _operatorName;

        public Task Completion => _block?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after create blocks method.");

        public FixedPointSource(ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            this.executionDataflowBlockOptions = executionDataflowBlockOptions;
        }

        internal ITargetBlock<KeyValuePair<int, IStreamEvent>> Target => _block!;

        internal IEnumerable<ITargetBlock<IStreamEvent>> Links => _links.Select(x => x.Item1);

        public int LinksCount => _links.Count;

        public string Name => _operatorName ?? throw new InvalidOperationException("Operator name must be set before use.");

        public string DisplayName => throw new NotImplementedException();

        public void Initialize()
        {
            _block = new TransformBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>(x => x.Value, executionDataflowBlockOptions);
            
            if (_links.Count > 1)
            {
                // If there are more than 1 link, we must broadcast the message to all targets
                var broadcastBlock = new GuaranteedBroadcastBlock<IStreamEvent>(executionDataflowBlockOptions);
                _source = broadcastBlock;
                _block.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            }
            else
            {
                _source = _block;
            }
        }

        public void Link()
        {
            Debug.Assert(_source != null);
            foreach (var link in _links)
            {
                _source.LinkTo(link.Item1, link.Item2);
            }
        }

        public void Complete()
        {
            Debug.Assert(_source != null);
            _source.Complete();
        }

        public IStreamEvent? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target, out bool messageConsumed)
        {
            Debug.Assert(_source != null);
            return _source.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(_source != null);
            _source.Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            _links.Add((target, linkOptions));
            return default!;
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_source != null);
            _source.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_source != null);
            return _source.ReserveMessage(messageHeader, target);
        }

        public void Setup(string streamName, string operatorName)
        {
            _operatorName = operatorName;
        }

        public Task Initialize(string name, long restoreTime, long newTime, JsonElement? state, IVertexHandler vertexHandler)
        {
            throw new NotImplementedException();
        }

        public void CreateBlock()
        {
            throw new NotImplementedException();
        }

        public Task Compact()
        {
            throw new NotImplementedException();
        }

        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return _links.Select(x => x.Item1);
        }

        public ValueTask DisposeAsync()
        {
            throw new NotImplementedException();
        }

        public void Pause()
        {
        }

        public void Resume()
        {
        }
    }
}
