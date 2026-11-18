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

using DataflowStream.dataflow.Internal;
using DataflowStream.dataflow.Internal.Extensions;
using FlowtideDotNet.Base.dataflow;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices
{
    /// <summary>
    /// A message of a round and the input it came from.
    /// </summary>
    public readonly struct RoundMessage<T>
    {
        public RoundMessage(int targetId, T message)
        {
            TargetId = targetId;
            Message = message;
        }

        public int TargetId { get; }

        public T Message { get; }
    }

    /// <summary>
    /// A vertex with several inputs that can hand its operator the data messages already queued on them in one call, a round.
    /// </summary>
    /// <typeparam name="T">The underlying data type of the stream messages being processed.</typeparam>
    /// <remarks>
    /// Watermarks, locking events and initial data done events are synchronized across the inputs as in <see cref="MultipleInputVertex{T}"/>.
    /// A round holds only messages that were queued ahead of the next event of another kind, so no message is taken past a watermark, a checkpoint or a trigger.
    /// With <see cref="MaxRoundMessages"/> at one the vertex calls <see cref="OnRecieve(int, T, long)"/> per message.
    /// </remarks>
    public abstract class MultipleInputRoundVertex<T> : ISourceBlock<IStreamEvent>, IStreamVertex, IStreamVertexCancellation
    {
        private readonly MultipleInputTargetHolder[] _targetHolders;
        private TransformManyBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>? _transformBlock;

        // Everything the inputs have queued, in the order it arrived
        private BufferBlock<KeyValuePair<int, IStreamEvent>>? _funnel;
        private Task? _funnelEnded;
        private Predicate<KeyValuePair<int, IStreamEvent>>? _fitsInRound;
        private long[]? _roundCost;
        private ISourceBlock<IStreamEvent>? _sourceBlock;

        private ILockingEvent?[]? _targetInCheckpoint;
        private ILockingEvent[]? _lastSeenCheckpointEvents;
        private readonly object _targetCheckpointLock;
        private Guid?[]? _targetLockingPrepare;
        private bool[]? _expectsLockingPrepare;

        private IReadOnlySet<string>[]? _targetWatermarkNames;
        private Watermark[]? _targetWatermarks;
        private Watermark? _currentWatermark;
        private Watermark? _previousWatermark;
        private bool[]? _targetSentDataSinceLastWatermark;
        private bool[]? _targetSentWatermark;
        private bool _isInIteration = false;

        private ParallelSource<IStreamEvent>? _parallelSource;
        private long _currentTime;
        private readonly int targetCount;
        private readonly ExecutionDataflowBlockOptions executionDataflowBlockOptions;
        private readonly List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)> _links = new List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)>();
        private bool _isHealthy = true;
        private CancellationTokenSource? tokenSource;
        private IMemoryAllocator? _memoryAllocator;
        private readonly PauseGate _pauseGate = new PauseGate();
        private bool _initialWatermarkSent;

        private string? _name;

        /// <summary>
        /// Gets the configured explicit identifier name of the vertex.
        /// </summary>
        public string Name => _name ?? throw new InvalidOperationException("Name can only be fetched after initialize or setup method calls");

        private string? _streamName;
        protected string StreamName => _streamName ?? throw new InvalidOperationException("StreamName can only be fetched after initialize or setup method calls");

        /// <summary>
        /// Gets the display name for the specific vertex type, largely used for logging and metrics visualization.
        /// </summary>
        public abstract string DisplayName { get; }

        private IMeter? _metrics;
        protected IMeter Metrics => _metrics ?? throw new InvalidOperationException("Metrics can only be fetched after or during initialize");

        private ILogger? _logger;

        /// <summary>
        /// Gets the logger instance associated with this vertex.
        /// </summary>
        public ILogger Logger => _logger ?? throw new InvalidOperationException("Logger can only be fetched after or during initialize");

        private StreamVersionInformation? _streamVersion;

        /// <summary>
        /// Gets the stream version information.
        /// </summary>
        public StreamVersionInformation? StreamVersion => _streamVersion;

        protected IMemoryAllocator MemoryAllocator => _memoryAllocator ?? throw new InvalidOperationException("Memory allocator can only be fetched after initialization.");

        // As the gauge: the block holds one event, what waits behind held output is behind the consumer
        protected float Backpressure
        {
            get
            {
                var block = _transformBlock ?? throw new InvalidOperationException("OutputCount can only be fetched after initialization.");
                return block.OutputCount > 0 ? ((float)(block.OutputCount + (_funnel?.Count ?? 0))) / executionDataflowBlockOptions.BoundedCapacity : 0;
            }
        }

#if DEBUG_WRITE
        // Debug data
        private StreamWriter? allInput;
        private bool _debugFileOpened;
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipleInputRoundVertex{T}"/> class.
        /// </summary>
        /// <param name="targetCount">The exact expected number of distinct upstream sources this operator must bind to.</param>
        /// <param name="executionDataflowBlockOptions">Configuration options covering block capacities and parallel properties.</param>
        protected MultipleInputRoundVertex(int targetCount, ExecutionDataflowBlockOptions executionDataflowBlockOptions)
        {
            _targetCheckpointLock = new object();
            var clone = executionDataflowBlockOptions.DefaultOrClone();
            clone.EnsureOrdered = true;
            this.targetCount = targetCount;
            this.executionDataflowBlockOptions = executionDataflowBlockOptions;
            _targetHolders = new MultipleInputTargetHolder[targetCount];

            for (int i = 0; i < targetCount; i++)
            {
                _targetHolders[i] = new MultipleInputTargetHolder(i, executionDataflowBlockOptions);
            }
        }

        private void InitializeBlock()
        {
            tokenSource = new CancellationTokenSource();
            executionDataflowBlockOptions.CancellationToken = tokenSource.Token;
            // One event in the block at a time: what waits stays in the funnel where a round can take it
            var transformOptions = executionDataflowBlockOptions.DefaultOrClone();
            transformOptions.BoundedCapacity = 1;
            _funnel = new BufferBlock<KeyValuePair<int, IStreamEvent>>(new DataflowBlockOptions()
            {
                BoundedCapacity = executionDataflowBlockOptions.BoundedCapacity,
                TaskScheduler = executionDataflowBlockOptions.TaskScheduler
            });
            _roundCost = new long[targetCount];
            _fitsInRound = FitsInRound;
            _transformBlock = new TransformManyBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>(CancellationFault.Wrap<KeyValuePair<int, IStreamEvent>, IStreamEvent>(this, tokenSource.Token, (r) =>
            {
                if (r.Value is ILockingEvent ev)
                {
#if DEBUG_WRITE
                    allInput?.WriteLine($"Received locking event {ev.GetType().Name} from target {r.Key}");
                    allInput?.Flush();
#endif
                    Logger.ReceivedLockingEventOnTarget(Name, ev.GetType().Name, r.Key);
                    if (TargetInCheckpoint(r.Key, ev, out var checkpoints))
                    {
                        _lastSeenCheckpointEvents = checkpoints;
                        if (_parallelSource == null)
                        {
                            return HandleCheckpointEnumerable(ev);
                        }
                        return Passthrough(r.Value);
                    }
                    return Empty();
                }
                if (r.Value is LockingEventPrepare lockingEventPrepare)
                {
                    return HandleLockingEventPrepare(r.Key, lockingEventPrepare);
                }
                if (r.Value is TriggerEvent triggerEvent)
                {
                    var enumerator = OnTrigger(triggerEvent.Name, triggerEvent.State);
                    if (_pauseGate.IsPaused)
                    {
                        enumerator = WaitForPause(enumerator);
                    }
                    return new AsyncEnumerableDowncast<T, IStreamEvent>(enumerator, (source) =>
                    {
                        if (source is IRentable rentable)
                        {
                            rentable.Rent(_links.Count);
                        }
                        return new StreamMessage<T>(source, _currentTime);
                    });
                }
                if (r.Value is StreamMessage<T> streamMessage)
                {
                    Debug.Assert(_targetSentDataSinceLastWatermark != null);
                    _targetSentDataSinceLastWatermark[r.Key] = true;
                    if (_parallelSource == null && MaxRoundMessages > 1)
                    {
                        var round = TakeRound(r.Key, streamMessage);
                        if (round != null)
                        {
                            return RunRound(round, streamMessage.Time);
                        }
                    }
                    var enumerator = OnRecieve(r.Key, streamMessage.Data, streamMessage.Time);
                    if (_pauseGate.IsPaused)
                    {
                        enumerator = WaitForPause(enumerator);
                    }

                    if (streamMessage.Data is IRentable rentable)
                    {
                        return new AsyncEnumerableReturnRentable<T, IStreamEvent>(rentable, enumerator, (source) =>
                        {
                            if (source is IRentable rentable)
                            {
                                rentable.Rent(_links.Count);
                            }
                            return new StreamMessage<T>(source, streamMessage.Time);
                        });
                    }
                    else
                    {
                        return new AsyncEnumerableDowncast<T, IStreamEvent>(enumerator, (source) =>
                        {
                            if (source is IRentable rentable)
                            {
                                rentable.Rent(_links.Count);
                            }
                            return new StreamMessage<T>(source, streamMessage.Time);
                        });
                    }
                }
                if (r.Value is Watermark watermark)
                {
                    return HandleWatermark(r.Key, watermark);
                }
                if (r.Value is InitialDataDoneEvent initialDataDone)
                {
                    return HandleInitialDataDoneEvent(r.Key, initialDataDone);
                }
                throw new NotSupportedException();
            }), transformOptions);
            _funnel.LinkTo(_transformBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            // However the block ends, by a failure of its own too: the funnel of that block takes no more and lets go of what it holds
            _funnelEnded = _transformBlock.Completion.ContinueWith(static (_, state) =>
            {
                var funnel = (BufferBlock<KeyValuePair<int, IStreamEvent>>)state!;
                funnel.Complete();
                return ReleaseUntilEnded(funnel);
            }, _funnel, CancellationToken.None, Common.GetContinuationOptions(), TaskScheduler.Default).Unwrap();

            if (executionDataflowBlockOptions.GetSupportsParallelExecution())
            {
                _parallelSource = new ParallelSource<IStreamEvent>(HandleCheckpoint, CheckpointSent, executionDataflowBlockOptions);

                _transformBlock.LinkTo(_parallelSource, new DataflowLinkOptions()
                {
                    PropagateCompletion = true
                });
                _sourceBlock = _parallelSource;
            }
            else
            {
                _sourceBlock = _transformBlock;
            }

            _targetInCheckpoint = new ILockingEvent[targetCount];
            _targetLockingPrepare = new Guid?[targetCount];
            _expectsLockingPrepare = new bool[targetCount];
            _targetWatermarks = new Watermark[targetCount];
            _targetSentDataSinceLastWatermark = new bool[targetCount];
            _targetSentWatermark = new bool[targetCount];
            _initialWatermarkSent = false;

            for (int i = 0; i < _targetSentDataSinceLastWatermark.Length; i++)
            {
                // We set this to true for the initial data, so all inputs have time to send their initial data before the first watermark is sent.
                // If a target does not send any data, it will still send an InitialDataDoneEvent which will handle that input.
                _targetSentDataSinceLastWatermark[i] = true;
            }

            foreach (var t in _targetHolders)
            {
                t.Initialize();
                // Not with its completion: a faulted funnel drops what it holds without returning it
                t.LinkTo(_funnel, new DataflowLinkOptions());
                t.Completion.ContinueWith(static (faulted, state) =>
                {
                    ((IDataflowBlock)state!).Fault(faulted.Exception!);
                }, _transformBlock, CancellationToken.None, Common.GetContinuationOptions() | TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
            }

            if (_links.Count > 1)
            {
                var broadcastBlock = new GuaranteedBroadcastBlock<IStreamEvent>(new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = executionDataflowBlockOptions.BoundedCapacity
                });
                _sourceBlock = broadcastBlock;
                if (_parallelSource != null)
                {
                    _parallelSource.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
                }
                else
                {
                    _transformBlock.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
                }
            }

            // Completion goes on with the blocks of this start as state, a vertex that was started again has new ones
            Task.WhenAll(_targetHolders.Select(x => x.Completion)).ContinueWith((completed, state) =>
            {
                var (funnel, block) = ((BufferBlock<KeyValuePair<int, IStreamEvent>>, TransformManyBlock<KeyValuePair<int, IStreamEvent>, IStreamEvent>))state!;
                if (completed.IsFaulted)
                {
                    (block as ISourceBlock<IStreamEvent>).Fault(completed.Exception!);
                }
                // The transform block completes when it has taken what the funnel still holds
                funnel.Complete();
            }, (_funnel, _transformBlock), CancellationToken.None, Common.GetContinuationOptions(), TaskScheduler.Default);
        }

        // Takes the data messages that follow in the funnel, up to the first event of another kind and the limits of a round. Null when none follows
        private List<RoundMessage<T>>? TakeRound(int targetId, StreamMessage<T> first)
        {
            Debug.Assert(_funnel != null && _roundCost != null && _targetSentDataSinceLastWatermark != null);
            Array.Clear(_roundCost);
            _roundCost[targetId] = RoundCost(first.Data);
            _roundCount = 1;
            _roundTime = first.Time;
            if (!_funnel.TryReceive(_fitsInRound, out var next))
            {
                return null;
            }
            var round = new List<RoundMessage<T>>() { new RoundMessage<T>(targetId, first.Data) };
            do
            {
                _targetSentDataSinceLastWatermark[next.Key] = true;
                round.Add(new RoundMessage<T>(next.Key, ((StreamMessage<T>)next.Value).Data));
                _roundCount++;
            }
            while (_funnel.TryReceive(_fitsInRound, out next));
            return round;
        }

        private int _roundCount;
        private long _roundTime;

        // Only the event at the head of the funnel is asked: a data message of the same time that fits is taken, anything else ends the round
        private bool FitsInRound(KeyValuePair<int, IStreamEvent> next)
        {
            if (next.Value is not StreamMessage<T> message || _roundCount >= MaxRoundMessages || message.Time != _roundTime)
            {
                return false;
            }
            long cost = _roundCost![next.Key] + RoundCost(message.Data);
            if (cost > MaxRoundCostPerInput && _roundCost[next.Key] > 0)
            {
                return false;
            }
            _roundCost[next.Key] = cost;
            return true;
        }

        private async IAsyncEnumerable<IStreamEvent> RunRound(List<RoundMessage<T>> round, long time)
        {
            try
            {
                var enumerator = OnRecieveRound(round, time);
                if (_pauseGate.IsPaused)
                {
                    enumerator = WaitForPause(enumerator);
                }
                await foreach (var e in enumerator)
                {
                    if (e is IRentable rentable)
                    {
                        rentable.Rent(_links.Count);
                    }
                    yield return new StreamMessage<T>(e, time);
                }
            }
            finally
            {
                // The round has ended, read to its end or not: the operator works on its messages no more
                for (int i = 0; i < round.Count; i++)
                {
                    if (round[i].Message is IRentable rentable)
                    {
                        rentable.Return();
                    }
                }
            }
        }

        // The messages still waiting are not handled any more, what they hold is let go of
        private void ReleaseQueued()
        {
            var funnel = _funnel;
            if (funnel != null)
            {
                ReleaseQueued(funnel);
            }
        }

        // An event an input was handing over when the funnel closed arrives after it, the funnel ends when that one is taken too
        private static async Task ReleaseUntilEnded(BufferBlock<KeyValuePair<int, IStreamEvent>> funnel)
        {
            do
            {
                ReleaseQueued(funnel);
            }
            while (await funnel.OutputAvailableAsync().ConfigureAwait(false));
        }

        private static void ReleaseQueued(BufferBlock<KeyValuePair<int, IStreamEvent>> funnel)
        {
            while (funnel.TryReceive(out var queued))
            {
                if (queued.Value is StreamMessage<T> message && message.Data is IRentable rentable)
                {
                    rentable.Return();
                }
            }
        }

        /// <summary>
        /// Events the inputs have handed over that wait for the one being handled.
        /// </summary>
        protected int QueuedEvents => _funnel?.Count ?? 0;

        /// <summary>
        /// The most data messages one round holds. One, the default, gives a call of <see cref="OnRecieve(int, T, long)"/> per message.
        /// </summary>
        protected virtual int MaxRoundMessages => 1;

        /// <summary>
        /// The most a round takes from one input, in the unit of <see cref="RoundCost(T)"/>. An input always gives at least one message.
        /// </summary>
        protected virtual long MaxRoundCostPerInput => long.MaxValue;

        /// <summary>
        /// What a message counts for against <see cref="MaxRoundCostPerInput"/>, for example its rows.
        /// </summary>
        protected virtual long RoundCost(T message) => 1;

        /// <summary>
        /// Handles the data messages that were queued together, in the order they arrived. The default handles them one by one.
        /// </summary>
        /// <param name="messages">Two or more messages, each with the input it came from.</param>
        /// <param name="time">The stream logical time of the messages.</param>
        public virtual async IAsyncEnumerable<T> OnRecieveRound(IReadOnlyList<RoundMessage<T>> messages, long time)
        {
            for (int i = 0; i < messages.Count; i++)
            {
                await foreach (var output in OnRecieve(messages[i].TargetId, messages[i].Message, time))
                {
                    yield return output;
                }
            }
        }

        private IAsyncEnumerable<IStreamEvent> HandleLockingEventPrepare(int targetId, LockingEventPrepare lockingEventPrepare)
        {
            Debug.Assert(_targetInCheckpoint != null);
            Debug.Assert(_targetLockingPrepare != null);
            Debug.Assert(_expectsLockingPrepare != null);
            // Set that this operator is in an iteration. This helps watermarks output.
            _isInIteration = true;

            _targetLockingPrepare[targetId] = lockingEventPrepare.Id;

            if (lockingEventPrepare.IsInitEvent)
            {
                _expectsLockingPrepare[targetId] = true;
            }

            // Check that all other inputs are waiting for checkpoint.
            bool allInCheckpoint = true;
            for (int i = 0; i < _targetInCheckpoint.Length; i++)
            {
                // Check if the target sent in a locking event prepare, if it did, it will not send a checkpoint event
                if (_targetLockingPrepare[i] != null)
                {
                    continue;
                }
                if (_targetInCheckpoint[i] == null)
                {
                    allInCheckpoint = false;
                }
            }
            if (!allInCheckpoint)
            {
                lockingEventPrepare.OtherInputsNotInCheckpoint = true;
            }


            if (!lockingEventPrepare.IsInitEvent)
            {
                // If it is not an init event, only one locking event prepare should be sent.
                // So this code makes sure that the last one is sent.
                // Check if we are still waiting for any locking event prepare, if so, skip sending this message
                for (int i = 0; i < _targetLockingPrepare.Length; i++)
                {
                    if (_expectsLockingPrepare[i] && (_targetLockingPrepare[i] != lockingEventPrepare.Id))
                    {
                        return EmptyAsyncEnumerable<IStreamEvent>.Instance;
                    }
                }
            }

            return FlushAndForwardPrepare(lockingEventPrepare);
        }

        private async IAsyncEnumerable<IStreamEvent> FlushAndForwardPrepare(LockingEventPrepare lockingEventPrepare)
        {
            await foreach (var e in OnLockingEventPrepare())
            {
                if (e is IRentable rentable)
                {
                    rentable.Rent(_links.Count);
                }
                yield return new StreamMessage<T>(e, _currentTime);
            }
            yield return lockingEventPrepare;
        }

        protected virtual IAsyncEnumerable<T> OnLockingEventPrepare()
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }

        private async IAsyncEnumerable<IStreamEvent> HandleInitialDataDoneEvent(int targetId, InitialDataDoneEvent initialDataDoneEvent)
        {
            Debug.Assert(_targetWatermarkNames != null, nameof(_targetWatermarkNames));
            Debug.Assert(_targetWatermarks != null, nameof(_targetWatermarks));
            Debug.Assert(_targetSentWatermark != null);
            Debug.Assert(_targetSentDataSinceLastWatermark != null);

            if (_initialWatermarkSent || _isInIteration)
            {
                yield break;
            }

            _targetSentWatermark[targetId] = true;

            for (int i = 0; i < _targetSentDataSinceLastWatermark.Length; i++)
            {
                if (_targetSentDataSinceLastWatermark[i] && !_targetSentWatermark[i])
                {
                    yield break;
                }
            }
            for (int i = 0; i < _targetSentDataSinceLastWatermark.Length; i++)
            {
                _targetSentDataSinceLastWatermark[i] = false;
                _targetSentWatermark[i] = false;
            }

            bool hasRecievedWatermark = false;

            for(int i = 0; i < _targetWatermarks.Length; i++)
            {
                if (_targetWatermarks[i] != null)
                {
                    hasRecievedWatermark = true;
                    break;
                }
            }
            _initialWatermarkSent = true;

            if (!hasRecievedWatermark || (_currentWatermark == null))
            {
                // If no watermark was sent, send an empty one
                yield return initialDataDoneEvent;
                yield break;
            }

            if (!_currentWatermark.Equals(_previousWatermark))
            {
                await foreach (var e in OnWatermark(_currentWatermark))
                {
                    if (e is IRentable rentable)
                    {
                        rentable.Rent(_links.Count);
                    }
                    yield return new StreamMessage<T>(e, _currentTime);
                }
                _previousWatermark = _currentWatermark;
            }

            yield return _currentWatermark;
        }

        private async IAsyncEnumerable<IStreamEvent> HandleWatermark(int targetId, Watermark watermark)
        {
            Debug.Assert(_targetWatermarkNames != null, nameof(_targetWatermarkNames));
            Debug.Assert(_targetWatermarks != null, nameof(_targetWatermarks));
            Debug.Assert(_targetSentWatermark != null);
            Debug.Assert(_targetSentDataSinceLastWatermark != null);

#if DEBUG_WRITE
            allInput?.WriteLine($"Watermark: {JsonSerializer.Serialize(watermark)} from target {targetId}");
            allInput?.Flush();
#endif

            if (_currentWatermark == null)
            {
                _currentWatermark = new Watermark(ImmutableDictionary<string, AbstractWatermarkValue>.Empty, watermark.StartTime)
                {
                    SourceOperatorId = watermark.SourceOperatorId
                };
                _previousWatermark = _currentWatermark;
            }
            _targetSentWatermark[targetId] = true;
            _targetWatermarks[targetId] = watermark;

            var currentDict = _currentWatermark.Watermarks;
            foreach (var kv in watermark.Watermarks)
            {
                AbstractWatermarkValue? watermarkValue = kv.Value;
                for (int i = 0; i < _targetWatermarkNames.Length; i++)
                {
                    // Check if any other target handles the same key
                    if (_targetWatermarkNames[i].Contains(kv.Key))
                    {
                        if (_targetWatermarks[i] != null &&
                            _targetWatermarks[i].Watermarks.TryGetValue(kv.Key, out var otherTargetOffset))
                        {
                            watermarkValue = AbstractWatermarkValue.Min(watermarkValue, otherTargetOffset);
                        }
                        else
                        {
                            watermarkValue = null;
                        }
                    }
                }
                if (watermarkValue != null)
                {
                    currentDict = currentDict.SetItem(kv.Key, watermarkValue);
                }
            }
            var newWatermark = new Watermark(currentDict, watermark.StartTime)
            {
                SourceOperatorId = watermark.SourceOperatorId
            };

            // only output watermark if there is a difference in the numbers
            if (!newWatermark.Equals(_currentWatermark))
            {
                _currentWatermark = newWatermark;
            }

            if (!_isInIteration)
            {
                for (int i = 0; i < _targetSentDataSinceLastWatermark.Length; i++)
                {
                    if (_targetSentDataSinceLastWatermark[i] && !_targetSentWatermark[i])
                    {
#if DEBUG_WRITE
                        allInput?.WriteLine($"Not sending watermark because target {i} has sent data but not watermark since last watermark");
                        allInput?.Flush();
#endif
                        yield break;
                    }
                }
                for (int i = 0; i < _targetSentDataSinceLastWatermark.Length; i++)
                {
                    _targetSentDataSinceLastWatermark[i] = false;
                    _targetSentWatermark[i] = false;
                }
            }

            // only output watermark if there is a difference in the numbers
            if (!_currentWatermark.Equals(_previousWatermark))
            {
                await foreach (var e in OnWatermark(newWatermark))
                {
                    if (e is IRentable rentable)
                    {
                        rentable.Rent(_links.Count);
                    }
                    yield return new StreamMessage<T>(e, _currentTime);
                }
                _previousWatermark = _currentWatermark;
                _initialWatermarkSent = true;
                yield return _currentWatermark;
            }
        }

        protected virtual IAsyncEnumerable<T> OnWatermark(Watermark watermark)
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }

        private async IAsyncEnumerable<IStreamEvent> HandleCheckpointEnumerable(ILockingEvent checkpointEvent)
        {
            if (checkpointEvent is ICheckpointEvent)
            {
                await foreach (var e in OnCheckpointFlush())
                {
                    if (e is IRentable rentable)
                    {
                        rentable.Rent(_links.Count);
                    }
                    yield return new StreamMessage<T>(e, _currentTime);
                }
            }
            var transformedEvent = await HandleCheckpoint(checkpointEvent);
            CheckpointSent();
            yield return transformedEvent;
        }

        /// <summary>
        /// Flush data before the aligned checkpoint event is forwarded, not called in parallel mode
        /// </summary>
        protected virtual IAsyncEnumerable<T> OnCheckpointFlush()
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }

        private async Task<ILockingEvent> HandleCheckpoint(ILockingEvent lockingEvent)
        {
            Debug.Assert(_targetLockingPrepare != null);
            // Reset all targets that was in the locking prepare state
            for (int i = 0; i < _targetLockingPrepare.Length; i++)
            {
                _targetLockingPrepare[i] = default;
            }

            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                _currentTime = checkpointEvent.NewTime;
                await OnCheckpoint();
                _lastSeenCheckpointEvents = null;
                return checkpointEvent;
            }
            if (lockingEvent is InitWatermarksEvent initWatermarksEvent)
            {
                Debug.Assert(_lastSeenCheckpointEvents != null, nameof(_lastSeenCheckpointEvents));

                HashSet<string> uniqueNames = new HashSet<string>();
                List<IReadOnlySet<string>> targetsWatermarks = new List<IReadOnlySet<string>>();
                foreach (var e in _lastSeenCheckpointEvents)
                {
                    if (e is InitWatermarksEvent previous)
                    {
                        foreach (var name in previous.WatermarkNames)
                        {
                            uniqueNames.Add(name);
                        }
                        initWatermarksEvent = initWatermarksEvent.AddWatermarkNames(previous.WatermarkNames);
                        targetsWatermarks.Add(previous.WatermarkNames);
                    }
                    else
                    {
                        targetsWatermarks.Add(new HashSet<string>());
                    }
                }
                _targetWatermarkNames = targetsWatermarks.ToArray();
                _currentWatermark = new Watermark(uniqueNames.Select(x => new KeyValuePair<string, AbstractWatermarkValue>(x, null!)).ToImmutableDictionary(), DateTimeOffset.UtcNow);


                return initWatermarksEvent;
            }
            return lockingEvent;
        }

        private void CheckpointSent()
        {
            foreach (var target in _targetHolders)
            {
                target.ReleaseCheckpoint();
            }
        }

        private IAsyncEnumerable<IStreamEvent> Passthrough(IStreamEvent streamEvent)
        {
            return new SingleAsyncEnumerable<IStreamEvent>(streamEvent);
        }

        private IAsyncEnumerable<IStreamEvent> Empty()
        {
            return EmptyAsyncEnumerable<IStreamEvent>.Instance;
        }

        /// <summary>
        /// Handles custom logic executed when the operator processes a queued trigger event.
        /// </summary>
        /// <param name="name">The name identifier distinguishing the associated trigger execution.</param>
        /// <param name="state">Associated state.</param>
        /// <returns>Asynchronous stream of response data payloads generated autonomously by the trigger.</returns>
        public virtual IAsyncEnumerable<T> OnTrigger(string name, object? state)
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }

        private async IAsyncEnumerable<T> WaitForPause(IAsyncEnumerable<T> input)
        {
            var task = _pauseGate.PauseTask;
            if (task != null)
            {
                await task;
            }

            await foreach (var element in input)
            {
                yield return element;
            }
        }

        private bool TargetInCheckpoint(int targetId, ILockingEvent checkpointEvent, out ILockingEvent[]? checkpoints)
        {
            lock (_targetCheckpointLock)
            {
                Debug.Assert(_targetInCheckpoint != null, nameof(_targetInCheckpoint));
                Logger.TargetInCheckpoint(targetId, StreamName, Name);
                _targetInCheckpoint[targetId] = checkpointEvent;

                bool allInCheckpoint = true;

                for (int i = 0; i < _targetInCheckpoint.Length; i++)
                {
                    if (_targetInCheckpoint[i] == null)
                    {
                        allInCheckpoint = false;
                        break;
                    }
                }

                if (allInCheckpoint)
                {
                    Logger.CheckpointInOperator(StreamName, Name);

                    for (int i = 0; i < _targetInCheckpoint.Length; i++)
                    {
                        if (_targetInCheckpoint[i]!.GetType() != checkpointEvent.GetType())
                        {
                            // The aligned locking events have different types, the inputs have
                            // received different amounts of locking events and every alignment
                            // from here on pairs unrelated events, which corrupts checkpoint
                            // consistency between the inputs.
                            Logger.LogError(
                                "Operator {operatorId} aligned locking events of different types, target {targetId} has {targetEventType} while target {completingTargetId} has {completingEventType}. The inputs have diverged in locking event counts.",
                                Name, i, _targetInCheckpoint[i]!.GetType().Name, targetId, checkpointEvent.GetType().Name);
                        }
                    }

                    // Create a new array here, have already checked that noone is null in the array
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
                    checkpoints = _targetInCheckpoint.ToArray();
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
                    // Reset
                    for (int i = 0; i < _targetInCheckpoint.Length; i++)
                    {
                        _targetInCheckpoint[i] = null;
                    }
                    return true;
                }
                checkpoints = null;
                return false;
            }
        }

        /// <summary>
        /// Executed when an actual checkpoint boundary occurs. Derived classes can clear state or commit offsets.
        /// </summary>
        public abstract Task OnCheckpoint();

        /// <summary>
        /// Subscribes to incoming payload data mapped to specific stream branches allowing the operator to parse multiplexed elements efficiently.
        /// </summary>
        /// <param name="targetId">The identifier reflecting which specific target/source yielded the event.</param>
        /// <param name="msg">The strongly-typed message payload received</param>
        /// <param name="time">The stream logical time when the message was generated.</param>
        /// <returns>An asynchronous stream of new outgoing payloads.</returns>
        public abstract IAsyncEnumerable<T> OnRecieve(int targetId, T msg, long time);

        /// <summary>
        /// Gets a task that represents the completion of the vertex processing block.
        /// </summary>
        public Task Completion => _transformBlock?.Completion ?? throw new InvalidOperationException("Completion can only be fetched after create blocks method.");

        /// <summary>
        /// The input targets for the vertex
        /// </summary>
        public MultipleInputTargetHolder[] Targets => _targetHolders;

        /// <summary>
        /// Signals to the dataflow block that it should not process any new elements and complete its execution gracefully.
        /// </summary>
        public void Complete()
        {
            Debug.Assert(_transformBlock != null, nameof(_transformBlock));

            foreach (var target in _targetHolders)
            {
                target.Complete();
            }
            if (tokenSource != null)
            {
                tokenSource.Cancel();
            }

            _funnel?.Complete();
            _transformBlock.Complete();
            ReleaseQueued();
        }

        /// <summary>
        /// Consumes a message from this block. Mostly used via underlying TPL dataflow interfaces.
        /// </summary>
        public IStreamEvent? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target, out bool messageConsumed)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            return _sourceBlock.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        Task IStreamVertexCancellation.CancelPendingOperations() =>
            tokenSource?.CancelAsync() ?? Task.CompletedTask;

        /// <summary>
        /// Puts the underlying block immediately into a faulted state due to a severe exception.
        /// </summary>
        /// <param name="exception">The triggering exception.</param>
        public void Fault(Exception exception)
        {
            if (tokenSource != null)
            {
                tokenSource.Cancel();
            }
            if (_transformBlock == null)
            {
                // The block is created first at start, a failure before that (for example
                // storage initialization) has nothing to fault.
                return;
            }
            (_transformBlock as IDataflowBlock).Fault(exception);
            _funnel?.Complete();
            ReleaseQueued();
        }

        /// <summary>
        /// Links the output of this vertex to a new target block.
        /// </summary>
        /// <param name="target">The target block.</param>
        /// <param name="linkOptions">Link behavior options.</param>
        /// <returns>Null, breaking a link is not possible in a stream</returns>
        public IDisposable LinkTo(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            _links.Add((target, linkOptions));
            return default!;
        }

        private IDisposable LinkTo_Internal(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            return _sourceBlock.LinkTo(target, linkOptions);
        }

        /// <summary>
        /// Releases a previously reserved message.
        /// </summary>
        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            if (_parallelSource != null)
            {
                (_parallelSource as ISourceBlock<IStreamEvent>).ReleaseReservation(messageHeader, target);
            }
            else
            {
                Debug.Assert(_transformBlock != null, nameof(_transformBlock));
                (_transformBlock as ISourceBlock<IStreamEvent>).ReleaseReservation(messageHeader, target);
            }
        }

        /// <summary>
        /// Reserves a message that is about to be consumed.
        /// </summary>
        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_sourceBlock != null, nameof(_sourceBlock));
            return (_sourceBlock as ISourceBlock<IStreamEvent>).ReserveMessage(messageHeader, target);
        }

        /// <summary>
        /// Asynchronously initializes the vertex, wiring up metrics, dependencies, and persistent state retrieval.
        /// </summary>
        /// <param name="name">The name assigned to the vertex.</param>
        /// <param name="restoreTime">The time representing the last known good state to restore from.</param>
        /// <param name="newTime">The new logical stream execution time.</param>
        /// <param name="vertexHandler">The handler containing stream environment references like state client and metrics.</param>
        /// <param name="streamVersionInformation">Configuration tracking the overall version of stream changes.</param>
        public Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler, StreamVersionInformation? streamVersionInformation)
        {
            _memoryAllocator = vertexHandler.MemoryManager;
            _name = name;
            _streamName = vertexHandler.StreamName;
            _metrics = vertexHandler.Metrics;
            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);
            _streamVersion = streamVersionInformation;

            Metrics.CreateObservableGauge("busy", () =>
            {
                Debug.Assert(_funnel != null && _transformBlock != null, nameof(_funnel));
                // The block holds one event: when its output is not taken, what waits is behind the consumer and not behind this vertex
                return _transformBlock.OutputCount > 0 ? 0 : ((float)_funnel.Count) / executionDataflowBlockOptions.BoundedCapacity;
            });
            Metrics.CreateObservableGauge("backpressure", () =>
            {
                Debug.Assert(_transformBlock != null && _funnel != null, nameof(_transformBlock));
                return _transformBlock.OutputCount > 0 ? ((float)(_transformBlock.OutputCount + _funnel.Count)) / executionDataflowBlockOptions.BoundedCapacity : 0;
            });
            Metrics.CreateObservableGauge("health", () =>
            {
                return _isHealthy ? 1 : 0;
            });
            Metrics.CreateObservableGauge("metadata", () =>
            {
                TagList tags = new TagList()
                {
                    { "id", Name }
                };
                var links = GetLinks();
                StringBuilder outputLinks = new StringBuilder();
                outputLinks.Append('[');
                foreach (var link in links)
                {
                    if (link is IStreamVertex streamVertex)
                    {
                        outputLinks.Append(streamVertex.Name);
                    }
                    else if (link is MultipleInputTargetHolder target)
                    {
                        outputLinks.Append(target.OperatorName);
                    }
                    outputLinks.Append(',');
                }
                outputLinks.Remove(outputLinks.Length - 1, 1);
                outputLinks.Append(']');
                tags.Add("links", outputLinks.ToString());
                return new Measurement<int>(1, tags);
            });

            Metrics.CreateObservableGauge("link", () =>
            {
                var links = GetLinks();

                List<Measurement<int>> measurements = new List<Measurement<int>>();

                foreach (var link in links)
                {
                    TagList tags = new TagList
                    {
                        { "source", Name }
                    };
                    if (link is IStreamVertex streamVertex)
                    {
                        tags.Add("target", streamVertex.Name);
                        tags.Add("id", streamVertex.Name + "-" + Name);
                    }
                    else if (link is MultipleInputTargetHolder target)
                    {
                        tags.Add("target", target.OperatorName);
                        tags.Add("id", target.OperatorName + "-" + Name);
                    }
                    measurements.Add(new Measurement<int>(1, tags));
                }
                return measurements;
            });

            _currentTime = newTime;


#if DEBUG_WRITE
            // Debug data
            if (allInput != null)
            {
                allInput.WriteLine("Restart");
            }
            else
            {
                if (!Directory.Exists("debugwrite"))
                {
                    Directory.CreateDirectory("debugwrite");
                }
                // Appended to, the file is closed when the vertex is disposed and a stream that recovers opens it again
                allInput = new StreamWriter($"debugwrite/{StreamName}-{Name}.vertex.txt", append: _debugFileOpened);
                if (_debugFileOpened)
                {
                    allInput.WriteLine("Restart");
                }
                _debugFileOpened = true;
            }
#endif

            return InitializeOrRestore(vertexHandler.StateClient);
        }

        protected void SetHealth(bool healthy)
        {
            _isHealthy = healthy;
        }

        protected abstract Task InitializeOrRestore(IStateManagerClient stateManagerClient);

        /// <summary>
        /// Requests the operator to compact its persistent storage.
        /// </summary>
        public abstract Task Compact();

        /// <summary>
        /// Enqueues an execution trigger.
        /// </summary>
        /// <param name="triggerEvent">The event data concerning the trigger schedule and state.</param>
        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            // Trigger injection happens at the first target.
            // This is done to make sure that it doesnt run if a checkpoint is being done.
            return _targetHolders.First().SendAsync(triggerEvent);
        }

        /// <summary>
        /// Disposes internal async resources asynchronously.
        /// </summary>
        public virtual async ValueTask DisposeAsync()
        {
            if (tokenSource != null)
            {
                tokenSource.Dispose();
                tokenSource = null;
            }
            var funnel = _funnel;
            var funnelEnded = _funnelEnded;
            var block = _transformBlock;
            if (funnel != null)
            {
                // Closed first: letting go of what waits must not make room for more
                foreach (var target in _targetHolders)
                {
                    target.Complete();
                }
                funnel.Complete();
                await ReleaseUntilEnded(funnel).ConfigureAwait(false);
                if (funnelEnded != null && (block?.Completion.IsCompleted ?? false))
                {
                    // The ending of the block lets go of events too, it may hold one it has not returned yet
                    await funnelEnded.ConfigureAwait(false);
                }
            }
#if DEBUG_WRITE
            allInput?.Dispose();
            allInput = null;
#endif
        }

        /// <summary>
        /// Translates logical linkages created over Setup/Initialize phases to physical Dataflow block links.
        /// </summary>
        public void Link()
        {
            if (_links.Count > 0)
            {
                foreach (var link in _links)
                {
                    LinkTo_Internal(link.Item1, link.Item2);
                }
            }
        }

        /// <summary>
        /// Instantiates underlying processing blocks. Must be called before linkages or message pushing.
        /// </summary>
        public void CreateBlock()
        {
            InitializeBlock();
        }

        /// <summary>
        /// Initiates the deletion process, allowing derived classes to clean up disk storage or other unmanaged environments permanently.
        /// </summary>
        public abstract Task DeleteAsync();

        /// <summary>
        /// Sets up the basic identifying information for the vertex within the pipeline.
        /// </summary>
        /// <param name="streamName">The globally scoped stream name.</param>
        /// <param name="operatorName">The explicit component name associated tightly with state storage.</param>
        public void Setup(string streamName, string operatorName)
        {
            _name = operatorName;
            _streamName = streamName;

            foreach (var target in Targets)
            {
                target.Setup(operatorName);
            }
        }

        /// <summary>
        /// Fetches the current targets attached to this node.
        /// </summary>
        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return _links.Select(x => x.Item1);
        }

        /// <summary>
        /// Halts current processing. Used during debug or suspension states.
        /// </summary>
        public void Pause()
        {
            _pauseGate.Pause();
        }

        /// <summary>
        /// Resumes processing of events queued while suspended.
        /// </summary>
        public void Resume()
        {
            _pauseGate.Resume();
        }

        /// <summary>
        /// A notification hook invoked prior to finalizing changes in persistent stores for a checkpoint.
        /// Can be used cautiously to record minimal forward state markers (e.g. streaming offsets).
        /// </summary>
        public virtual Task BeforeSaveCheckpoint()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Indicates a rollback behavior hook whenever a previous version is targeted for restoring due to errors.
        /// </summary>
        /// <param name="rollbackVersion">The version that the stream will be rolled back to.</param>
        public virtual Task OnFailure(long rollbackVersion)
        {
            return Task.CompletedTask;
        }
    }
}
