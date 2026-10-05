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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Check
{
    /// <summary>
    /// Committed counts of a check operator, indexed like its checks.
    /// </summary>
    internal sealed class ColumnCheckOperatorState
    {
        public long[] ActiveIssues { get; set; } = Array.Empty<long>();

        public long[] FailingRows { get; set; } = Array.Empty<long>();
    }

    /// <summary>
    /// Passes rows through and tracks the issues and counts of its checks.
    /// </summary>
    internal sealed class ColumnCheckOperator : UnaryVertex<StreamEventBatch>
    {
        private sealed class CheckInstance
        {
            public CheckInstance(string checkId, CheckDefinition definition, IFunctionsRegister functionsRegister)
            {
                CheckId = checkId;
                CheckName = definition.Message;
                IsFailing = CheckConditionCompiler.CompileFailing(definition, functionsRegister);
                TagKeys = new string[definition.Tags.Count];
                TagWriters = new Action<EventBatchData, int, Column>[definition.Tags.Count];
                for (int i = 0; i < definition.Tags.Count; i++)
                {
                    TagKeys[i] = definition.Tags[i].Key;
                    TagWriters[i] = ColumnProjectCompiler.Compile(definition.Tags[i].Value, functionsRegister);
                }
                KeyColumnCount = Math.Max(1, TagKeys.Length);
                Sorter = new BatchSorter(KeyColumnCount);
                GaugeTags = new KeyValuePair<string, object?>[]
                {
                    new KeyValuePair<string, object?>("check_name", CheckName),
                    new KeyValuePair<string, object?>("check_id", checkId)
                };
            }

            public string CheckId { get; }

            public string CheckName { get; }

            public Func<EventBatchData, int, bool> IsFailing { get; }

            public string[] TagKeys { get; }

            public Action<EventBatchData, int, Column>[] TagWriters { get; }

            /// <summary>
            /// The tag values, or one constant null column for a tagless check.
            /// </summary>
            public int KeyColumnCount { get; }

            public BatchSorter Sorter { get; }

            public KeyValuePair<string, object?>[] GaugeTags { get; }

            public int[] FailingRowIndices = Array.Empty<int>();

            public int FailingCount;

            public readonly CheckCounts Counts = new CheckCounts();

            // Copies of Counts for the gauge collecting thread
            public long ActiveIssuesMirror;

            public long FailingRowsMirror;

            public CheckStatus LastReportedStatus;

            public IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? Tree;

            public IBPlusTreeBulkInserter<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? Inserter;

            public IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? Pending;

            public IBPlusTreeBulkInserter<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? PendingInserter;

            public bool PendingDirty;
        }

        private readonly int[]? _emit;
        private readonly ICheckIssuePublisher _publisher;
        private readonly CheckInstance[] _checks;
        private readonly IObjectColumnConverter _tagConverter;
        private readonly DataValueContainer _valueContainer = new DataValueContainer();

        private IObjectState<ColumnCheckOperatorState>? _state;
        private long _checkpointVersion;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;
        private IObservableGauge<long>? _activeIssuesGauge;
        private IObservableGauge<long>? _failingRowsGauge;

        private ColumnRowReference[] _keys = Array.Empty<ColumnRowReference>();
        private int[] _values = Array.Empty<int>();
        private int[] _keyWeights = Array.Empty<int>();
        private int[] _sortedIndices = Array.Empty<int>();
        private int[] _duplicateTags = Array.Empty<int>();
        private int[] _uniqueIndices = Array.Empty<int>();
        private int[] _uniqueTags = Array.Empty<int>();
        private int[] _transitions = Array.Empty<int>();

        public ColumnCheckOperator(
            CheckRelation relation,
            IFunctionsRegister functionsRegister,
            ICheckIssuePublisher publisher,
            IReadOnlyList<string> checkIds,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            if (checkIds.Count != relation.Checks.Count)
            {
                throw new ArgumentException("There must be one check id per check.", nameof(checkIds));
            }
            _publisher = publisher;
            if (relation.EmitSet)
            {
                _emit = relation.Emit.ToArray();
            }
            _checks = new CheckInstance[relation.Checks.Count];
            for (int i = 0; i < _checks.Length; i++)
            {
                _checks[i] = new CheckInstance(checkIds[i], relation.Checks[i], functionsRegister);
            }
            _tagConverter = new ObjectConverterResolver().GetConverter(ObjectConverterTypeInfoLookup.GetTypeInfo(typeof(object)));
        }

        public override string DisplayName => "Check";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        internal protected override Task<ILockingEvent> HandleCheckpoint(ILockingEvent lockingEvent)
        {
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                // Batches sealed at this barrier publish once it commits
                _checkpointVersion = checkpointEvent.CheckpointVersion;
            }
            return base.HandleCheckpoint(lockingEvent);
        }

        public override async Task OnCheckpoint()
        {
            Debug.Assert(_state?.Value != null);
            for (int i = 0; i < _checks.Length; i++)
            {
                await SealCheck(_checks[i]);
            }
            var state = _state.Value;
            for (int i = 0; i < _checks.Length; i++)
            {
                var check = _checks[i];
                Debug.Assert(check.Tree != null);
                await check.Tree.Commit();
                state.ActiveIssues[i] = check.Counts.ActiveIssues;
                state.FailingRows[i] = check.Counts.FailingRows;
            }
            await _state.Commit();
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            var data = msg.Data;
            var count = data.Count;
            var batch = data.EventBatchData;
            _eventsProcessed.Add(count);
            _eventsCounter.Add(count);

            bool anyFailing = false;
            for (int c = 0; c < _checks.Length; c++)
            {
                var check = _checks[c];
                if (check.FailingRowIndices.Length < count)
                {
                    check.FailingRowIndices = new int[count];
                }
                var failingRows = check.FailingRowIndices;
                var isFailing = check.IsFailing;
                int failing = 0;
                for (int i = 0; i < count; i++)
                {
                    if (isFailing(batch, i))
                    {
                        failingRows[failing++] = i;
                    }
                }
                check.FailingCount = failing;
                anyFailing |= failing > 0;
            }

            if (!anyFailing)
            {
                return new SingleAsyncEnumerable<StreamEventBatch>(CreateOutput(msg));
            }
            return ApplyFailures(msg);
        }

        private async IAsyncEnumerable<StreamEventBatch> ApplyFailures(StreamEventBatch msg)
        {
            var data = msg.Data;
            for (int c = 0; c < _checks.Length; c++)
            {
                var check = _checks[c];
                if (check.FailingCount > 0)
                {
                    await ApplyCheck(check, data);
                    check.FailingCount = 0;
                }
            }
            yield return CreateOutput(msg);
        }

        /// <summary>
        /// The input itself, or its emit reusing the input columns.
        /// </summary>
        private StreamEventBatch CreateOutput(StreamEventBatch msg)
        {
            if (_emit == null)
            {
                return msg;
            }
            var data = msg.Data;
            var columns = new IColumn[_emit.Length];
            for (int i = 0; i < _emit.Length; i++)
            {
                columns[i] = data.EventBatchData.Columns[_emit[i]];
            }
            return new StreamEventBatch(new EventBatchWeighted(data.Weights, data.Iterations, new EventBatchData(columns)));
        }

        private async ValueTask ApplyCheck(CheckInstance check, EventBatchWeighted data)
        {
            Debug.Assert(check.Inserter != null);

            var batch = data.EventBatchData;
            var keyColumns = new IColumn[check.KeyColumnCount];
            for (int i = 0; i < keyColumns.Length; i++)
            {
                keyColumns[i] = Column.Create(MemoryAllocator);
            }

            int rows = 0;
            try
            {
                if (check.TagWriters.Length == 0)
                {
                    // Every failing row nets into the one constant key
                    int weight = 0;
                    for (int j = 0; j < check.FailingCount; j++)
                    {
                        weight += data.Weights[check.FailingRowIndices[j]];
                    }
                    EnsureCapacity(1);
                    keyColumns[0].Add(NullValue.Instance);
                    _keyWeights[rows++] = weight;
                }
                else
                {
                    EnsureCapacity(check.FailingCount);

                    // Key row j is the j-th failing row
                    for (int j = 0; j < check.FailingCount; j++)
                    {
                        var row = check.FailingRowIndices[j];
                        for (int t = 0; t < check.TagWriters.Length; t++)
                        {
                            check.TagWriters[t](batch, row, (Column)keyColumns[t]);
                        }
                        _keyWeights[rows++] = data.Weights[row];
                    }
                }

                var keyBatch = new EventBatchData(keyColumns);
                for (int j = 0; j < rows; j++)
                {
                    _sortedIndices[j] = j;
                    _keys[j] = new ColumnRowReference() { referenceBatch = keyBatch, RowIndex = j };
                }
                SortKeys(check, keyColumns, rows);

                // One net weight per key, net zero keys are skipped
                int unique = 0;
                int start = 0;
                while (start < rows)
                {
                    var tag = _duplicateTags[start];
                    var representative = _sortedIndices[start];
                    var net = _keyWeights[representative];
                    var end = start + 1;
                    while (end < rows && _duplicateTags[end] == tag)
                    {
                        net += _keyWeights[_sortedIndices[end]];
                        end++;
                    }
                    if (net != 0)
                    {
                        _uniqueIndices[unique++] = representative;
                        _values[representative] = net;
                        _transitions[representative] = 0;
                    }
                    start = end;
                }
                if (unique == 0)
                {
                    return;
                }

                var byteSize = keyBatch.GetByteSize() + (rows * sizeof(int));
                await check.Inserter.ApplyBatch(_keys, _values, unique, _uniqueIndices, _uniqueTags, new CheckWeightMutator(_transitions, check.Counts), byteSize);
                Volatile.Write(ref check.ActiveIssuesMirror, check.Counts.ActiveIssues);
                Volatile.Write(ref check.FailingRowsMirror, check.Counts.FailingRows);

                if (!_publisher.IssuesEnabled)
                {
                    return;
                }
                Debug.Assert(check.PendingInserter != null);

                // Keep the keys that changed activity, still in sorted order
                int transitionCount = 0;
                for (int k = 0; k < unique; k++)
                {
                    var representative = _uniqueIndices[k];
                    var transition = _transitions[representative];
                    if (transition != 0)
                    {
                        _uniqueIndices[transitionCount++] = representative;
                        _values[representative] = transition;
                    }
                }
                if (transitionCount == 0)
                {
                    return;
                }

                await check.PendingInserter.ApplyBatch(_keys, _values, transitionCount, _uniqueIndices, _uniqueTags, new CheckPendingMutator(), byteSize);
                check.PendingDirty = true;
            }
            finally
            {
                Array.Clear(_keys, 0, rows);
                for (int i = 0; i < keyColumns.Length; i++)
                {
                    keyColumns[i].Dispose();
                }
            }
        }

        private void SortKeys(CheckInstance check, IColumn[] keyColumns, int rows)
        {
            var sortedSpan = _sortedIndices.AsSpan(0, rows);
            var tagsSpan = _duplicateTags.AsSpan(0, rows);
            check.Sorter.SortDataWithTags(keyColumns, ref sortedSpan, ref tagsSpan);
        }

        private void EnsureCapacity(int rows)
        {
            if (_keys.Length >= rows)
            {
                return;
            }
            _keys = new ColumnRowReference[rows];
            _values = new int[rows];
            _keyWeights = new int[rows];
            _sortedIndices = new int[rows];
            _duplicateTags = new int[rows];
            _uniqueIndices = new int[rows];
            _transitions = new int[rows];
            _uniqueTags = new int[rows];
            for (int i = 0; i < rows; i++)
            {
                _uniqueTags[i] = i;
            }
        }

        /// <summary>
        /// Enqueues the issue changes since the last seal and the status when the counts changed.
        /// </summary>
        private async ValueTask SealCheck(CheckInstance check)
        {
            List<CheckIssueChange>? changes = null;
            if (check.PendingDirty)
            {
                Debug.Assert(check.Pending != null);
                using (var iterator = check.Pending.CreateIterator())
                {
                    await iterator.SeekFirst();
                    await foreach (var page in iterator)
                    {
                        var keys = page.Keys;
                        var values = page.Values;
                        for (int i = 0; i < keys.Count; i++)
                        {
                            var delta = values.Get(i);
                            if (delta != 0)
                            {
                                changes ??= new List<CheckIssueChange>();
                                changes.Add(CreateChange(check, keys.Data, i, delta > 0));
                            }
                        }
                    }
                }
                await check.Pending.Clear();
                check.PendingDirty = false;
            }

            CheckStatus? status = null;
            if (_publisher.StatusEnabled)
            {
                var current = new CheckStatus(check.Counts.ActiveIssues, check.Counts.FailingRows);
                if (current != check.LastReportedStatus)
                {
                    status = current;
                    check.LastReportedStatus = current;
                }
            }

            if (changes != null || status.HasValue)
            {
                _publisher.Enqueue(new CheckIssueBatch()
                {
                    CheckId = check.CheckId,
                    CheckName = check.CheckName,
                    Version = _checkpointVersion,
                    IsSnapshot = false,
                    Changes = changes ?? (IReadOnlyList<CheckIssueChange>)Array.Empty<CheckIssueChange>(),
                    Status = status
                });
            }
        }

        /// <summary>
        /// Publishes every active issue of the restored, already committed tree.
        /// </summary>
        private async ValueTask PublishSnapshot(CheckInstance check, CheckStatus? status)
        {
            Debug.Assert(check.Tree != null);

            var issues = new List<CheckIssueChange>();
            using (var iterator = check.Tree.CreateIterator())
            {
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    var keys = page.Keys;
                    var values = page.Values;
                    for (int i = 0; i < keys.Count; i++)
                    {
                        if (values.Get(i) > 0)
                        {
                            issues.Add(CreateChange(check, keys.Data, i, true));
                        }
                    }
                }
            }
            // Sent even when empty, it resets the listener's view
            _publisher.Publish(new CheckIssueBatch()
            {
                CheckId = check.CheckId,
                CheckName = check.CheckName,
                Version = _checkpointVersion,
                IsSnapshot = true,
                Changes = issues,
                Status = status
            });
        }

        private CheckIssueChange CreateChange(CheckInstance check, EventBatchData keys, int index, bool active)
        {
            var tags = check.TagKeys.Length == 0 ? Array.Empty<KeyValuePair<string, object?>>() : new KeyValuePair<string, object?>[check.TagKeys.Length];
            for (int i = 0; i < check.TagKeys.Length; i++)
            {
                keys.Columns[i].GetValueAt(index, _valueContainer, default);
                tags[i] = new KeyValuePair<string, object?>(check.TagKeys[i], _tagConverter.Deserialize(_valueContainer));
            }
            return new CheckIssueChange(active, tags);
        }

        /// <summary>
        /// One measurement per check from the mirrors, runs on the collecting thread.
        /// </summary>
        private IEnumerable<Measurement<long>> ObserveCounts(bool failingRows)
        {
            var measurements = new Measurement<long>[_checks.Length];
            for (int i = 0; i < _checks.Length; i++)
            {
                var check = _checks[i];
                var value = failingRows ? Volatile.Read(ref check.FailingRowsMirror) : Volatile.Read(ref check.ActiveIssuesMirror);
                measurements[i] = new Measurement<long>(value, check.GaugeTags);
            }
            return measurements;
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            // Meters outlive restarts
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            if (_activeIssuesGauge == null)
            {
                _activeIssuesGauge = Metrics.CreateObservableGauge<long>("check_active_issues", () => ObserveCounts(false));
            }
            if (_failingRowsGauge == null)
            {
                _failingRowsGauge = Metrics.CreateObservableGauge<long>("check_failing_rows", () => ObserveCounts(true));
            }

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<ColumnCheckOperatorState>("check_counts");
            var state = _state.Value ?? new ColumnCheckOperatorState();
            if (state.ActiveIssues.Length != _checks.Length || state.FailingRows.Length != _checks.Length)
            {
                // Checks without committed counts start at zero
                var activeIssues = state.ActiveIssues;
                var failingRows = state.FailingRows;
                Array.Resize(ref activeIssues, _checks.Length);
                Array.Resize(ref failingRows, _checks.Length);
                state.ActiveIssues = activeIssues;
                state.FailingRows = failingRows;
            }
            _state.Value = state;

            for (int i = 0; i < _checks.Length; i++)
            {
                var check = _checks[i];
                // Unpublished batches belong to an uncommitted epoch
                _publisher.DiscardPending(check.CheckId);

                check.Counts.ActiveIssues = state.ActiveIssues[i];
                check.Counts.FailingRows = state.FailingRows[i];
                Volatile.Write(ref check.ActiveIssuesMirror, check.Counts.ActiveIssues);
                Volatile.Write(ref check.FailingRowsMirror, check.Counts.FailingRows);
                var status = new CheckStatus(check.Counts.ActiveIssues, check.Counts.FailingRows);
                check.LastReportedStatus = status;
                check.FailingCount = 0;
                check.PendingDirty = false;

                check.Tree = await stateManagerClient.GetOrCreateTree($"check_{i}_v2", CreateTreeOptions(check.KeyColumnCount));
                check.Inserter = check.Tree.CreateBulkInserter();

                if (_publisher.IssuesEnabled)
                {
                    check.Pending = await stateManagerClient.GetOrCreateTree($"check_{i}_pending_v2", CreateTreeOptions(check.KeyColumnCount));
                    await check.Pending.Clear();
                    check.PendingInserter = check.Pending.CreateBulkInserter();
                    await PublishSnapshot(check, _publisher.StatusEnabled ? status : null);
                }
                else if (_publisher.StatusEnabled)
                {
                    _publisher.Publish(new CheckIssueBatch()
                    {
                        CheckId = check.CheckId,
                        CheckName = check.CheckName,
                        Version = _checkpointVersion,
                        IsSnapshot = false,
                        Changes = Array.Empty<CheckIssueChange>(),
                        Status = status
                    });
                }
            }
        }

        /// <summary>
        /// The running counts of a check, for tests.
        /// </summary>
        internal CheckStatus GetCountsForTests(int checkIndex)
        {
            var counts = _checks[checkIndex].Counts;
            return new CheckStatus(counts.ActiveIssues, counts.FailingRows);
        }

        /// <summary>
        /// Recounts a check from a full scan of its issue tree, for tests.
        /// </summary>
        internal async Task<CheckStatus> ScanCountsForTests(int checkIndex)
        {
            var tree = _checks[checkIndex].Tree;
            Debug.Assert(tree != null);

            long activeIssues = 0;
            long failingRows = 0;
            using (var iterator = tree.CreateIterator())
            {
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    var values = page.Values;
                    for (int i = 0; i < page.Keys.Count; i++)
                    {
                        var weight = values.Get(i);
                        if (weight > 0)
                        {
                            activeIssues++;
                            failingRows += weight;
                        }
                    }
                }
            }
            return new CheckStatus(activeIssues, failingRows);
        }

        private BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> CreateTreeOptions(int columnCount)
        {
            return new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(columnCount),
                KeySerializer = new ColumnStoreSerializer(columnCount, MemoryAllocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                UseByteBasedPageSizes = true,
                MemoryAllocator = MemoryAllocator
            };
        }
    }
}
