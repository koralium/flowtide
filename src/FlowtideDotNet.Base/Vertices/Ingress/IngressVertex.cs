﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.Ingress
{
    internal class IngressState<TData>
    {
        public BufferBlock<IStreamEvent>? _block;
        public ISourceBlock<IStreamEvent>? _sourceBlock;
        public long _currentTime;
        public long _restoreTime;
        public IVertexHandler? _vertexHandler;
        public SemaphoreSlim? _checkpointLock;
        public bool _inCheckpointLock;
        public IngressOutput<TData>? _output;
        public CancellationTokenSource? _tokenSource;
        public IMeter? _metrics;
        public bool _taskEnabled = false;
        public int _linkCount;
    }

    public abstract class IngressVertex<TData> : ISourceBlock<IStreamEvent>, IStreamIngressVertex
    {
        private readonly object _stateLock;
        private readonly DataflowBlockOptions options;
        private readonly List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)> _links = new List<(ITargetBlock<IStreamEvent>, DataflowLinkOptions)>();
        private IngressState<TData>? _ingressState;
        private readonly Dictionary<int, Task> _runningTasks;
        private int _taskIdCounter;
        private ILogger? _logger;
        private bool _isHealthy = true;

        public string Name { get; private set; }

        protected string StreamName { get; private set; }

        public StreamVersionInformation? StreamVersion { get; private set; }

        public abstract string DisplayName { get; }

        protected IMeter Metrics => _ingressState?._metrics ?? throw new NotSupportedException("Initialize must be called before accessing metrics");

        public ILogger Logger => _logger ?? throw new NotSupportedException("Logging must be done after Initialize");

        protected IMemoryAllocator MemoryAllocator => _ingressState?._vertexHandler?.MemoryManager ?? throw new NotSupportedException("Initialize must be called before accessing memory allocator");

        protected IngressVertex(DataflowBlockOptions options)
        {
            _stateLock = new object();
            this.options = options;
            _runningTasks = new Dictionary<int, Task>();
            Name = "";
            StreamName = "";
        }

        private void InitializeBlock()
        {
            lock (_stateLock)
            {
                if (options.CancellationToken.IsCancellationRequested)
                {
                    throw new InvalidOperationException("ExecutionDataflowBlockOptions CancellationToken is already cancalled, can not create the block");
                }
                _ingressState = new IngressState<TData>();
                _ingressState._checkpointLock = new SemaphoreSlim(1, 1);
                _ingressState._block = new BufferBlock<IStreamEvent>(options);
                _ingressState._linkCount = _links.Count;

                ISourceBlock<IStreamEvent> source = _ingressState._block;

                if (_links.Count > 1)
                {
                    var broadcastBlock = new GuaranteedBroadcastBlock<IStreamEvent>(new ExecutionDataflowBlockOptions()
                    {
                        MaxDegreeOfParallelism = 1,
                        BoundedCapacity = options.BoundedCapacity
                    });
                    source = broadcastBlock;
                    _ingressState._block.LinkTo(broadcastBlock, new DataflowLinkOptions() { PropagateCompletion = true });
                }

                _ingressState._sourceBlock = source;
                _ingressState._output = new IngressOutput<TData>(_ingressState, _ingressState._block);
                _ingressState._tokenSource = new CancellationTokenSource();
                _ingressState._block.Completion.ContinueWith(t =>
                {
                    Logger.LogDebug(t.Exception, "Block failure");
                    lock (_stateLock)
                    {
                        _ingressState._taskEnabled = false;
                    }

                    _ingressState._tokenSource.Cancel();
                });
            }
        }

        public Task Completion => GetCompletion();

        private Task GetCompletion()
        {
            lock (_stateLock)
            {
                Debug.Assert(_ingressState != null, nameof(_ingressState));
                Debug.Assert(_ingressState._block != null, nameof(_ingressState._block));

                List<Task> tasks = new List<Task>();
                tasks.Add(_ingressState._block.Completion);

                foreach (var task in _runningTasks.Values)
                {
                    tasks.Add(task);
                }

                return Task.WhenAll(tasks);
            }
        }

        protected abstract Task OnCheckpoint(long checkpointTime);

        public void Complete()
        {
            Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));
            Debug.Assert(_ingressState?._tokenSource != null, nameof(_ingressState._tokenSource));

            lock (_stateLock)
            {
                _ingressState._taskEnabled = false;
                _ingressState._tokenSource.Cancel();
                _ingressState._block.Complete();
            }
        }

        public IStreamEvent? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target, out bool messageConsumed)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));

            return _ingressState._sourceBlock.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));
            Debug.Assert(_ingressState?._tokenSource != null, nameof(_ingressState._tokenSource));
            lock (_stateLock)
            {
                _ingressState._taskEnabled = false;
                _ingressState._tokenSource.Cancel();
                (_ingressState._block as IDataflowBlock).Fault(exception);
            }
        }

        public IDisposable LinkTo(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            _links.Add((target, linkOptions));
            return default!;
        }

        private void LinkTo_Internal(ITargetBlock<IStreamEvent> target, DataflowLinkOptions linkOptions)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));
            _ingressState._sourceBlock.LinkTo(target, linkOptions);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));
            _ingressState._sourceBlock.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<IStreamEvent> target)
        {
            Debug.Assert(_ingressState?._sourceBlock != null, nameof(_ingressState._sourceBlock));
            return _ingressState._sourceBlock.ReserveMessage(messageHeader, target);
        }

        protected abstract Task SendInitial(IngressOutput<TData> output);

        public abstract Task Compact();

        private async Task RunLockingEvent(IngressOutput<TData> output, object? state)
        {
            Debug.Assert(_ingressState?._checkpointLock != null, nameof(_ingressState._checkpointLock));
            Debug.Assert(_ingressState?._output != null, nameof(_ingressState._output));

            var lockingEvent = (ILockingEvent)state!;
            await _ingressState._checkpointLock.WaitAsync(_ingressState._output.CancellationToken);
            _ingressState._inCheckpointLock = true;
            bool isStopStreamEvent = false;
            if (lockingEvent is ICheckpointEvent checkpoint)
            {
                if (checkpoint is StopStreamCheckpoint)
                {
                    isStopStreamEvent = true;
                    output.Stop();
                }
                await OnCheckpoint(checkpoint.CheckpointTime);
                await output.SendLockingEvent(lockingEvent);
            }
            if (lockingEvent is InitWatermarksEvent initWatermark)
            {
                var names = await GetWatermarkNames();
                await output.SendLockingEvent(initWatermark.AddWatermarkNames(names));
            }

            if (!isStopStreamEvent)
            {
                _ingressState._inCheckpointLock = false;
                _ingressState._checkpointLock.Release();
            }
        }

        protected abstract Task<IReadOnlySet<string>> GetWatermarkNames();

        public void DoLockingEvent(ILockingEvent lockingEvent)
        {
            RunTask(RunLockingEvent, lockingEvent);
        }

        protected void ScheduleCheckpoint(TimeSpan inTime)
        {
            Debug.Assert(_ingressState?._vertexHandler != null, nameof(_ingressState._vertexHandler));

            if (_ingressState._vertexHandler == null)
            {
                throw new NotSupportedException("Cannot schedule checkpoint before initialize");
            }
            _ingressState._vertexHandler.ScheduleCheckpoint(inTime);
        }

        private sealed record TaskState(Func<IngressOutput<TData>, object?, Task> func, IngressOutput<TData> ingressOutput, object? state, int taskId);

        /// <summary>
        /// Start a task that can output data to the stream.
        /// </summary>
        /// <param name="task"></param>
        /// <param name="state"></param>
        protected Task RunTask(Func<IngressOutput<TData>, object?, Task> task, object? state = null, TaskCreationOptions taskCreationOptions = TaskCreationOptions.None)
        {
            Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));
            Debug.Assert(_ingressState._output != null, nameof(_ingressState._output));

            TaskState tState;
            lock (_stateLock)
            {
                if (_ingressState._block.Completion.IsFaulted || !_ingressState._taskEnabled)
                {
                    return Task.CompletedTask;
                }

                var taskId = _taskIdCounter++;
                tState = new TaskState(task, _ingressState._output, state, taskId);
                var t = Task.Factory.StartNew((state) =>
                {
                    var taskState = (TaskState)state!;
                    return taskState.func(taskState.ingressOutput, taskState.state);
                }, tState, _ingressState._output.CancellationToken, taskCreationOptions, TaskScheduler.Default)
                .Unwrap();


                _runningTasks.Add(taskId, t);
                t.ContinueWith((task, state) =>
                {
                    var taskState = (TaskState)state!;
                    if (t.IsFaulted)
                    {
                        taskState.ingressOutput.Fault(task.Exception ?? new AggregateException("Error in task without exception"));
                    }
                    lock (_stateLock)
                    {
                        _runningTasks.Remove(taskState.taskId);
                    }
                }, tState, default, TaskContinuationOptions.None, TaskScheduler.Default);


                return t;
            }
        }

        public Task InitializationCompleted()
        {
            return RunTask(async (output, state) =>
            {
                await SendInitial(output);
                // Send event here that initial is completed
                await output.SendEvent(new InitialDataDoneEvent());
            }, taskCreationOptions: TaskCreationOptions.LongRunning);
        }

        public async Task Initialize(string name, long restoreTime, long newTime, IVertexHandler vertexHandler, StreamVersionInformation? streamVersionInformation)
        {
            Debug.Assert(_ingressState != null, nameof(_ingressState));

            Name = name;
            StreamName = vertexHandler.StreamName;
            StreamVersion = streamVersionInformation;

            if (_runningTasks.Count > 0)
            {
                throw new InvalidOperationException("Initialize while there are running tasks");
            }

            _logger = vertexHandler.LoggerFactory.CreateLogger(DisplayName);

            _ingressState._vertexHandler = vertexHandler;
            _ingressState._currentTime = newTime;
            _ingressState._restoreTime = restoreTime;
            _ingressState._metrics = vertexHandler.Metrics;

            Metrics.CreateObservableGauge("backpressure", () =>
            {
                Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));

                return ((float)_ingressState._block.Count) / options.BoundedCapacity;
            });
            Metrics.CreateObservableGauge<float>("busy", () =>
            {
                Debug.Assert(_ingressState?._block != null, nameof(_ingressState._block));

                var backpressurevalue = ((float)_ingressState._block.Count) / options.BoundedCapacity;
                if (_runningTasks.Count > 0)
                {
                    return (1.0f - backpressurevalue);
                }
                return 0.0f;
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

            await InitializeOrRestore(restoreTime, vertexHandler.StateClient);

            lock (_stateLock)
            {
                _ingressState._taskEnabled = true;
            }
        }

        protected void SetHealth(bool healthy)
        {
            _isHealthy = healthy;
        }

        protected abstract Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient);

        public Task QueueTrigger(TriggerEvent triggerEvent)
        {
            return OnTrigger(triggerEvent.Name, triggerEvent.State);
        }

        public abstract Task OnTrigger(string triggerName, object? state);

        protected Task RegisterTrigger(string name, TimeSpan? scheduleInterval = null)
        {
            Debug.Assert(_ingressState != null, nameof(_ingressState));

            if (_ingressState._vertexHandler == null)
            {
                throw new NotSupportedException("Cannot register trigger before initialize is called");
            }
            return _ingressState._vertexHandler.RegisterTrigger(name, scheduleInterval);
        }

        public virtual ValueTask DisposeAsync()
        {
            if (_ingressState != null)
            {
                if (_ingressState._output != null)
                {
                    _ingressState._output.Dispose();
                }
                if (_ingressState._inCheckpointLock && _ingressState._checkpointLock != null)
                {
                    _ingressState._inCheckpointLock = false;
                    _ingressState._checkpointLock.Release();
                }
            }

            return ValueTask.CompletedTask;
        }

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

        public void CreateBlock()
        {
            InitializeBlock();
        }

        public abstract Task DeleteAsync();

        public void Setup(string streamName, string operatorName)
        {
            Name = operatorName;
            StreamName = streamName;
        }

        public IEnumerable<ITargetBlock<IStreamEvent>> GetLinks()
        {
            return _links.Select(x => x.Item1);
        }

        public virtual Task CheckpointDone(long checkpointVersion)
        {
            return Task.CompletedTask;
        }

        public void Pause()
        {
            Debug.Assert(_ingressState?._output != null, nameof(_ingressState._output));
            _ingressState._output.Stop();
        }

        public void Resume()
        {
            Debug.Assert(_ingressState?._output != null, nameof(_ingressState._output));
            _ingressState._output.Resume();
        }

        public virtual Task BeforeSaveCheckpoint()
        {
            return Task.CompletedTask;
        }
    }
}
