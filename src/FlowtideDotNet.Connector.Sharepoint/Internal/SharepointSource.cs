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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Graph;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal class SharepointSourceState {

    }

    internal class SharepointSource : ReadBaseOperator<SharepointSourceState>
    {
        private readonly SharepointSourceOptions sharepointSourceOptions;
        private readonly ReadRelation readRelation;
        private SharepointGraphListClient? sharepointGraphListClient;
        private string? listId;

        public SharepointSource(SharepointSourceOptions sharepointSourceOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            this.sharepointSourceOptions = sharepointSourceOptions;
            this.readRelation = readRelation;
            
        }

        public override string DisplayName => "SharepointList";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
        }

        protected override async Task InitializeOrRestore(long restoreTime, SharepointSourceState? state, IStateManagerClient stateManagerClient)
        {
            sharepointGraphListClient = new SharepointGraphListClient(sharepointSourceOptions, StreamName, Name, Logger);
            await sharepointGraphListClient.Initialize();
            listId = await sharepointGraphListClient.GetListId(readRelation.NamedTable.DotSeperated);
        }

        protected override Task<SharepointSourceState> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new SharepointSourceState());
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(sharepointGraphListClient != null);
            Debug.Assert(listId != null);

            var iterator = sharepointGraphListClient.GetDeltaFromList(listId);
            await foreach(var page in iterator)
            {
            }
            throw new NotImplementedException();
        }
    }
}
