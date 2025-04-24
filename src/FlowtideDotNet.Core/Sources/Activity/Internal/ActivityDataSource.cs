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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Sources.Generic.Internal;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Activity.Internal
{
    internal class ActivityDataSource : ReadBaseOperator
    {
        private readonly ActivityListener _activityListener;
        private readonly string _watermarkName;
        private SemaphoreSlim _semaphore;
        private Queue<System.Diagnostics.Activity> _activityQueue;
        private readonly object _queueLock = new object();
        public ActivityDataSource(ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            _semaphore = new SemaphoreSlim(0);
            _activityQueue = new Queue<System.Diagnostics.Activity>();
            _watermarkName = readRelation.NamedTable.DotSeperated;
            _activityListener = new ActivityListener();
            _activityListener.ShouldListenTo = (source) => true;
            _activityListener.ActivityStopped = (activity) =>
            {
                lock (_queueLock)
                {
                    _activityQueue.Enqueue(activity);
                    _semaphore.Release();
                }
            };
        }

        public override string DisplayName => "ActivitySource";

        public override Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        public override Task CheckpointDone(long checkpointVersion)
        {
            return base.CheckpointDone(checkpointVersion);
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "delta_load")
            {
                RunTask(HandleDeltaLoad);
            }
            return Task.CompletedTask;
        }

        private async Task HandleDeltaLoad(IngressOutput<StreamEventBatch> output, object? state)
        {
            while (true)
            {
                output.CancellationToken.ThrowIfCancellationRequested();

                await _semaphore.WaitAsync(output.CancellationToken);

                Column[] columns = new Column[4];
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i] = Column.Create(MemoryAllocator);
                }   

                while (true)
                {
                    System.Diagnostics.Activity? activity = default;
                    lock (_queueLock)
                    {
                        if (_activityQueue.Count == 0)
                        {
                            break;
                        }
                        activity = _activityQueue.Dequeue();
                    }
                    columns[0].Add(new StringValue(activity.OperationName));
                    columns[1].Add(new StringValue(activity.Source.Name));
                    columns[2].Add(new StringValue(activity.DisplayName));
                    
                    List<KeyValuePair<IDataValue, IDataValue>> tagValues = new List<KeyValuePair<IDataValue, IDataValue>>();
                    AddTagsToList(tagValues, activity);
                    columns[3].Add(new MapValue(tagValues));
                }
                
            }
        }

        private void AddTagsToList(List<KeyValuePair<IDataValue, IDataValue>> tagValues, System.Diagnostics.Activity activity)
        {
            var unionResolver = new ObjectConverterResolver();
            var objectConverter = unionResolver.GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(typeof(object)));
            foreach (var tag in activity.TagObjects)
            {
                if (tag.Value == null)
                {
                    tagValues.Add(new KeyValuePair<IDataValue, IDataValue>(new StringValue(tag.Key), NullValue.Instance));
                }
                else
                {
                    var func = new AddToColumnFunc();
                    objectConverter.Serialize(tag.Value, ref func);
                    tagValues.Add(new KeyValuePair<IDataValue, IDataValue>(new StringValue(tag.Key), func.BoxedValue!));
                }
            }
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { _watermarkName });
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            throw new NotImplementedException();
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            throw new NotImplementedException();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await RegisterTrigger("delta_load", TimeSpan.FromSeconds(1));
        }
    }
}
