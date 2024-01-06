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
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Read
{
    public class BatchableReadOperatorState
    {
        public long LastWatermark { get; set; }
    }

    public record struct BatchableReadEvent(string Key, RowEvent RowEvent, long Watermark);

    public abstract class BatchableReadBaseOperator : ReadBaseOperator<BatchableReadOperatorState>
    {
        /// <summary>
        /// Temporary tree used to store the full load data
        /// </summary>
        private IBPlusTree<string, RowEvent>? _fullLoadTempTree;

        /// <summary>
        /// Persistent tree used to store the data
        /// </summary>
        private IBPlusTree<string, RowEvent>? _persistentTree;

        /// <summary>
        /// Tree used to store the deletions for the data in full load
        /// </summary>
        private IBPlusTree<string, int>? _deletionsTree;
        //private List<int> keyIndices;
        private BatchableReadOperatorState? _state;
        private string _watermarkName;
        public BatchableReadBaseOperator(ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            _watermarkName = readRelation.NamedTable.DotSeperated;
        }

        public override string DisplayName => "Generic";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            switch (triggerName)
            {
                case "full_load":
                    return RunTask(FullLoadTrigger);
                case "delta_load":
                    return RunTask(DeltaLoadTrigger);
            }
            return Task.CompletedTask;
        }

        private async Task FullLoadTrigger(IngressOutput<StreamEventBatch> output, object? state)
        {
            await DoFullLoad(output);
        }

        private async Task DeltaLoadTrigger(IngressOutput<StreamEventBatch> output, object? state)
        {
            await DoDeltaLoad(output);
        }

        protected override async Task InitializeOrRestore(long restoreTime, BatchableReadOperatorState? state, IStateManagerClient stateManagerClient)
        {
            if (state != null)
            {
                _state = state;
            }
            else
            {
                _state = new BatchableReadOperatorState()
                {
                    LastWatermark = -1
                };
            }

            _fullLoadTempTree = await stateManagerClient.GetOrCreateTree("full_load_temp", new BPlusTreeOptions<string, RowEvent>()
            {
                Comparer = StringComparer.Ordinal,
                KeySerializer = new StringSerializer(),
                ValueSerializer = new StreamEventBPlusTreeSerializer()
            });
            await _fullLoadTempTree.Clear();

            _persistentTree = await stateManagerClient.GetOrCreateTree("persistent", new BPlusTreeOptions<string, RowEvent>()
            {
                Comparer = StringComparer.Ordinal,
                KeySerializer = new StringSerializer(),
                ValueSerializer = new StreamEventBPlusTreeSerializer()
            });

            _deletionsTree = await stateManagerClient.GetOrCreateTree("deletions", new BPlusTreeOptions<string, int>()
            {
                Comparer = StringComparer.Ordinal,
                KeySerializer = new StringSerializer(),
                ValueSerializer = new IntSerializer()
            });
            await _deletionsTree.Clear();
        }

        private async IAsyncEnumerable<KeyValuePair<string, RowEvent>> IteratePerRow(IBPlusTreeIterator<string, RowEvent> iterator)
        {
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    yield return kv;
                }
            }
        }

        private async Task DoDeltaLoad(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_persistentTree != null, nameof(_persistentTree));
            Debug.Assert(_state != null, nameof(_state));
            await output.EnterCheckpointLock();
            long maxWatermark = _state.LastWatermark;
            List<RowEvent> outputList = new List<RowEvent>();
            bool sentUpdates = false;
            await foreach (var e in DeltaLoad(_state.LastWatermark))
            {
                var key = e.Key;
                if (e.RowEvent.Weight < 0)
                {
                    await _persistentTree.RMW(key, e.RowEvent, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            outputList.Add(new RowEvent(-1, 0, current.RowData));
                            return (current, GenericWriteOperation.Delete);
                        }
                        return (input, GenericWriteOperation.None);
                    });
                }
                else
                {
                    await _persistentTree.RMW(key, e.RowEvent, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            outputList.Add(new RowEvent(-1, 0, current.RowData));
                            outputList.Add(new RowEvent(1, 0, input.RowData));
                            return (input, GenericWriteOperation.Upsert);
                        }
                        outputList.Add(new RowEvent(1, 0, input.RowData));
                        return (input, GenericWriteOperation.Upsert);
                    });
                }

                maxWatermark = Math.Max(maxWatermark, e.Watermark);

                if (outputList.Count > 100)
                {
                    await output.SendAsync(new StreamEventBatch(outputList));
                    outputList = new List<RowEvent>();
                    sentUpdates = true;
                }
            }
            if (outputList.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(outputList));
                outputList = new List<RowEvent>();
                sentUpdates = true;
            }
            if (sentUpdates)
            {
                await output.SendWatermark(new Base.Watermark(_watermarkName, maxWatermark));
            }
            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }

        private async Task DoFullLoad(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_fullLoadTempTree != null, nameof(_fullLoadTempTree));
            Debug.Assert(_persistentTree != null, nameof(_persistentTree));
            Debug.Assert(_deletionsTree != null, nameof(_deletionsTree));
            Debug.Assert(_state != null, nameof(_state));

            // Lock checkpointing until the full load is complete
            await output.EnterCheckpointLock();

            long maxWatermark = 0;
            List<RowEvent> outputList = new List<RowEvent>();

            await foreach (var e in FullLoad())
            {
                if (e.RowEvent.Weight < 0)
                {
                    throw new NotSupportedException("Full load does not support deletions");
                }
                var key = e.Key;
                await _fullLoadTempTree.Upsert(key, e.RowEvent);
                var (op, oldData) = await _persistentTree.RMW(key, e.RowEvent, (input, current, exist) =>
                {
                    if (exist)
                    {
                        if (RowEvent.Compare(input, current) != 0)
                        {
                            outputList.Add(new RowEvent(1, 0, input.RowData));
                            outputList.Add(new RowEvent(-1, 0, current.RowData));
                            return (input, GenericWriteOperation.Upsert);
                        }
                        else
                        {
                            return (current, GenericWriteOperation.None);
                        }
                    }
                    outputList.Add(new RowEvent(1, 0, input.RowData));
                    return (input, GenericWriteOperation.Upsert);
                });

                maxWatermark = Math.Max(maxWatermark, e.Watermark);

                if (outputList.Count > 100)
                {
                    await output.SendAsync(new StreamEventBatch(outputList));
                    outputList = new List<RowEvent>();
                }
            }

            if (outputList.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(outputList));
                outputList = new List<RowEvent>();
            }

            var tmpIterator = _fullLoadTempTree.CreateIterator();
            var persistentIterator = _persistentTree.CreateIterator();
            await tmpIterator.SeekFirst();
            await persistentIterator.SeekFirst();

            var tmpEnumerator = IteratePerRow(tmpIterator).GetAsyncEnumerator();
            var persistentEnumerator = IteratePerRow(persistentIterator).GetAsyncEnumerator();

            var hasNew = await tmpEnumerator.MoveNextAsync();
            var hasOld = await persistentEnumerator.MoveNextAsync();

            // Go through both trees and find deletions
            while (hasNew || hasOld)
            {
                int comparison = hasNew && hasOld ? tmpEnumerator.Current.Key.CompareTo(persistentEnumerator.Current.Key) : 0;

                // If there is no more old data, then we are done
                if (!hasOld)
                {
                    break;
                }
                if (hasNew && comparison < 0)
                {
                    hasNew = await tmpEnumerator.MoveNextAsync();
                }
                if (!hasNew || comparison > 0)
                {
                    await _deletionsTree.Upsert(persistentEnumerator.Current.Key, 1);
                    // Deletion
                    outputList.Add(new RowEvent(-1, 0, persistentEnumerator.Current.Value.RowData));
                    hasOld = await persistentEnumerator.MoveNextAsync();
                }
                else
                {
                    hasNew = await tmpEnumerator.MoveNextAsync();
                    hasOld = await persistentEnumerator.MoveNextAsync();
                }
            }

            // Clear the temp tree
            await _fullLoadTempTree.Clear();

            var deleteIterator = _deletionsTree.CreateIterator();
            await deleteIterator.SeekFirst();

            await foreach (var page in deleteIterator)
            {
                foreach (var kv in page)
                {
                    // Go through the deletions and delete them from the persistent tree
                    await _persistentTree.RMW(kv.Key, default, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            // Output delete event
                            outputList.Add(new RowEvent(-1, 0, current.RowData));
                            return (current, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.None);
                    });

                    if (outputList.Count > 100)
                    {
                        await output.SendAsync(new StreamEventBatch(outputList));
                        outputList = new List<RowEvent>();
                    }
                }
            }
            // Clear the deletions tree
            await _deletionsTree.Clear();

            if (outputList.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(outputList));
                outputList = new List<RowEvent>();
            }
            // Send the new max watermark
            _state.LastWatermark = maxWatermark;
            await output.SendWatermark(new Base.Watermark(_watermarkName, maxWatermark));

            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }

        protected abstract IAsyncEnumerable<BatchableReadEvent> FullLoad();

        protected abstract IAsyncEnumerable<BatchableReadEvent> DeltaLoad(long lastWatermark);

        protected virtual TimeSpan? GetFullLoadSchedule()
        {
            return default;
        }

        protected abstract TimeSpan? GetDeltaLoadTimeSpan();

        protected override async Task<BatchableReadOperatorState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null, nameof(_state));
            Debug.Assert(_persistentTree != null, nameof(_persistentTree));

            await _persistentTree.Commit();
            return _state;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state != null, nameof(_state));
            if (_state.LastWatermark < 0)
            {
                // Only do full load if we have not done it before
                await DoFullLoad(output);
            }
            // Register full load trigger if the user wants to call it or schedule it
            await RegisterTrigger("full_load", GetFullLoadSchedule());
            // Register delta load trigger if the user wants to call it or schedule it
            await RegisterTrigger("delta_load", GetDeltaLoadTimeSpan());
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _watermarkName });
        }
    }
}
