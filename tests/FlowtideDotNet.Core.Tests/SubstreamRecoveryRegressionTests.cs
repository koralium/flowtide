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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Regression tests for the failure handling of the substream exchange. The failure
    /// state machine retries stop and dispose until every operator handled its failure, an
    /// exception from these paths therefore wedges the stream in an endless recovery loop,
    /// which froze whole clusters before: a fail and recover request from another substream
    /// arriving before this substreams exchange had initialized used to throw.
    /// </summary>
    // Serialized with FetchLoopStallWatchdogTests, both mutate the process-global
    // SubstreamCommunicationPoint.StallLimit/StallCheckInterval statics (see the collection
    // note there); running them concurrently leaks a non-default value across tests.
    [Collection("SubstreamStallStatics")]
    public class SubstreamRecoveryRegressionTests
    {
        private class RecordingHandler : ISubstreamCommunicationHandler
        {
            public Func<RecoveryWave, Task>? CallFailAndRecover;
            public Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>>? GetData;
            public Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>>? InitializeFromTarget;
            public int SendFailAndRecoverCalls;

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<RecoveryWave, Task> callFailAndRecover,
                Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                CallFailAndRecover = callFailAndRecover;
                GetData = getDataFunction;
                InitializeFromTarget = initializeFromTarget;
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                return Task.CompletedTask;
            }

            public void InitializeDurabilityClaims(Func<long, int, long, RecoveryWave, long, long, bool, Task> callReceiveDurabilityClaim)
            {
            }

            public Task SendDurabilityClaim(long version, int radius, long initVersion, RecoveryWave wave, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken) => Task.CompletedTask;

            public Task SendFailAndRecover(RecoveryWave wave)
            {
                Interlocked.Increment(ref SendFailAndRecoverCalls);
                return Task.CompletedTask;
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, RecoveryWave wave, CancellationToken cancellationToken)
            {
                return Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
            }
        }

        /// <summary>
        /// A fail and recover from the other substream before any exchange operator has
        /// registered must complete without an error, there is nothing running that needs to
        /// roll back, the initialize handshake reconciles the versions when the stream
        /// starts.
        /// </summary>
        [Fact]
        public async Task FailAndRecoverBeforeAnyOperatorRegisteredIsIgnored()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);

            Assert.NotNull(handler.CallFailAndRecover);
            await handler.CallFailAndRecover!(new RecoveryWave(5, Guid.NewGuid()));
        }

        /// <summary>
        /// Peer must retry until this stream restarts into its wave
        /// </summary>
        [Fact]
        public async Task HandshakeInAWaveNotRestartedIntoYetIsRetried()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            // Start a run, then enter the peer's wave
            communicationPoint.Waves.ForStart();
            var wave = new RecoveryWave(1, Guid.NewGuid());
            Assert.True(communicationPoint.Waves.TryEnter(wave));

            var beforeRestart = await handler.InitializeFromTarget!(1, 1, false, wave);
            Assert.True(beforeRestart.NotStarted);

            // Restart into the wave, now the retry is answered
            communicationPoint.Waves.ForStart();
            var afterRestart = await handler.InitializeFromTarget!(1, 1, false, wave);
            Assert.False(afterRestart.NotStarted);
        }

        /// <summary>
        /// A handshake from an older wave is also held
        /// </summary>
        [Fact]
        public async Task HandshakeInOlderWaveWhileRestartPendingIsHeld()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);

            var w1 = new RecoveryWave(1, Guid.NewGuid());
            Assert.True(communicationPoint.Waves.TryEnter(w1));
            communicationPoint.Waves.ForStart();

            // Fail locally into w2 but do not restart yet
            var w2 = communicationPoint.Waves.ForFailure();
            Assert.True(w2 > w1);
            Assert.True(communicationPoint.Waves.RestartPending);
            communicationPoint.OnStreamFailure();

            // A handshake sent in w1 must be held
            var beforeRestart = await handler.InitializeFromTarget!(1, 1, false, w1);
            Assert.True(beforeRestart.NotStarted);
        }

        /// <summary>
        /// A held handshake dispatches a failed restart again
        /// </summary>
        [Fact]
        public async Task AFailedRestartDispatchIsDispatchedAgainByAHeldHandshake()
        {
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            var target = new SubstreamTarget(1, 1, point, () => { });
            int dispatches = 0;
            target.SetRollback(_ =>
            {
                if (Interlocked.Increment(ref dispatches) == 1)
                {
                    throw new InvalidOperationException("the first dispatch fails");
                }
                return Task.CompletedTask;
            });
            point.Waves.ForStart();

            // The first restart dispatch from this handshake fails
            var wave = new RecoveryWave(1, Guid.NewGuid());
            Assert.True((await handler.InitializeFromTarget!(1, 1, false, wave)).NotStarted);
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (Volatile.Read(ref dispatches) < 2 && DateTime.UtcNow < deadline)
            {
                // Retries are held while the restart is pending
                Assert.True((await handler.InitializeFromTarget!(1, 1, false, wave)).NotStarted);
                await Task.Delay(10);
            }
            Assert.Equal(2, Volatile.Read(ref dispatches));
        }

        /// <summary>
        /// A fetch gets nothing from the ended run until restart
        /// </summary>
        [Fact]
        public async Task FetchWhileARestartIsPendingGetsNothingFromTheEndedRun()
        {
            using var meter = new Meter(nameof(FetchWhileARestartIsPendingGetsNothingFromTheEndedRun));
            using var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions(), NullLoggerFactory.Instance, meter, "stream", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            var target = new SubstreamTarget(1, 1, point, () => { });
            await target.Initialize(0, 1, stateManager.GetOrCreateClient("target"), new ExchangeOperatorState(),
                GlobalMemoryManager.Instance, _ => Task.CompletedTask, TimeSpan.FromSeconds(30));
            point.Waves.ForStart();

            // Add a real barrier, then enter a newer wave
            var barrier = new Checkpoint(1, 2, 1);
            await target.OnLockingEvent(barrier);
            Assert.True(point.Waves.TryEnter(new RecoveryWave(point.Waves.Current.Counter + 1, Guid.NewGuid())));
            var targetIds = new HashSet<int> { 1 };
            var fenced = await handler.GetData!(targetIds, 10, CancellationToken.None);
            try
            {
                Assert.Empty(fenced);

                // After the restart the barrier is still there
                point.Waves.ForStart();
                var released = await handler.GetData!(targetIds, 10, CancellationToken.None);
                try
                {
                    Assert.Same(barrier, Assert.Single(released).StreamEvent);
                }
                finally
                {
                    SubstreamEventWireSerializer.ReturnEvents(released);
                }
            }
            finally
            {
                SubstreamEventWireSerializer.ReturnEvents(fenced);
            }
        }

        private sealed class HookLogger : ILogger
        {
            public Action<string>? OnMessage;

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                OnMessage?.Invoke(formatter(state, exception));
            }
        }

        /// <summary>
        /// Refuse a clean handoff if a wave comes after the accept
        /// </summary>
        [Fact]
        public async Task CleanHandoffDecidedBeforeAWaveEntryIsNotAcceptedInIt()
        {
            using var meter = new Meter(nameof(CleanHandoffDecidedBeforeAWaveEntryIsNotAcceptedInIt));
            using var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions(), NullLoggerFactory.Instance, meter, "stream", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var logger = new HookLogger();
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(logger, "substream_0", "substream_1", handler);
            var target = new SubstreamTarget(1, 1, point, () => { });
            await target.Initialize(0, 1, stateManager.GetOrCreateClient("target"), new ExchangeOperatorState(),
                GlobalMemoryManager.Instance, _ => Task.CompletedTask, TimeSpan.FromSeconds(30));
            point.Waves.ForStart();
            point.Waves.StartCompleted();
            Assert.False(point.Waves.InInit);
            Assert.False(point.Waves.RestartPending);
            Assert.True((await handler.InitializeFromTarget!(1, 1, false, point.Waves.Current)).Success);
            await target.OnLockingEvent(new Checkpoint(1, 2, 1));
            var fetched = await handler.GetData!(new HashSet<int> { 1 }, 10, CancellationToken.None);
            Assert.Single(fetched);
            SubstreamEventWireSerializer.ReturnEvents(fetched);

            var peers = new RecoveryWave(point.Waves.Current.Counter + 1, Guid.NewGuid());
            logger.OnMessage = message =>
            {
                if (message.Contains("reconnected from a clean handoff", StringComparison.Ordinal))
                {
                    Assert.True(point.Waves.TryEnter(peers));
                }
            };
            var handoff = await handler.InitializeFromTarget!(1, 1, true, point.Waves.Current);
            logger.OnMessage = null;

            Assert.True(point.Waves.RestartPending);
            Assert.False(handoff.CleanReconnect);
            Assert.True(handoff.NotStarted);
        }

        private sealed class OperatorHost : OperatorTestBase
        {
        }

        /// <summary>
        /// Do not resume a clean handoff checked in an older run
        /// </summary>
        [Fact]
        public async Task CleanHandoffCheckedInOneRunIsNotResumedInTheNext()
        {
            using var meter = new Meter(nameof(CleanHandoffCheckedInOneRunIsNotResumedInTheNext));
            using var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions(), NullLoggerFactory.Instance, meter, "stream", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            var target = new SubstreamTarget(1, 1, point, () => { });
            await target.Initialize(0, 1, stateManager.GetOrCreateClient("target"), new ExchangeOperatorState(),
                GlobalMemoryManager.Instance, _ => Task.CompletedTask, TimeSpan.FromSeconds(30));
            using var host = new OperatorHost();
            var reader = new SubstreamReadOperator(point, new SubstreamExchangeReferenceRelation { SubStreamName = "substream_1", ExchangeTargetId = 1 }, new DataflowBlockOptions());
            await host.InitializeOperator(reader);
            point.Waves.ForStart();
            point.Waves.StartCompleted();
            Assert.True((await handler.InitializeFromTarget!(1, 1, false, point.Waves.Current)).Success);
            await target.OnLockingEvent(new Checkpoint(1, 2, 1));
            var fetched = await handler.GetData!(new HashSet<int> { 1 }, 10, CancellationToken.None);
            Assert.Single(fetched);
            SubstreamEventWireSerializer.ReturnEvents(fetched);
            // Pretend the reader consumed the stop barrier
            typeof(SubstreamReadOperator).GetField("_peerStopConsumed", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(reader, true);

            int armed = 1;
            int resumed = 0;
            SubstreamReadOperator.BeforeResumeHookForTests = _ =>
            {
                if (Interlocked.Exchange(ref armed, 0) != 1)
                {
                    return;
                }
                // Fail and restart the reader before the resume
                Task.Run(async () =>
                {
                    await reader.OnFailure(0);
                    reader.Complete();
                    try { await reader.Completion.WaitAsync(TimeSpan.FromSeconds(10)); }
                    catch (OperationCanceledException) { }
                    await reader.DisposeAsync();
                    reader.CreateBlock();
                    await host.ReinitializeOperator(reader);
                }).GetAwaiter().GetResult();
            };
            SubstreamReadOperator.ResumedHookForTests = (_, resumedReader) =>
            {
                if (ReferenceEquals(resumedReader, reader))
                {
                    Interlocked.Increment(ref resumed);
                }
            };
            SubstreamInitializeResponse handoff;
            try
            {
                handoff = await handler.InitializeFromTarget!(1, 1, true, point.Waves.Current);
            }
            finally
            {
                SubstreamReadOperator.BeforeResumeHookForTests = null;
                SubstreamReadOperator.ResumedHookForTests = null;
            }

            Assert.Equal(0, Volatile.Read(ref armed));
            Assert.Equal(0, Volatile.Read(ref resumed));
            Assert.False(point.IsSubscribed(1));
            Assert.False(handoff.CleanReconnect);
            Assert.True(handoff.NotStarted);

            reader.Complete();
            try { await reader.Completion.WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (OperationCanceledException) { }
            await reader.DisposeAsync();
        }

        /// <summary>
        /// An old wave failing late leaves the new wave alone
        /// </summary>
        [Fact]
        public async Task AnOlderWavesDelayedDispatchFailureDoesNotMarkTheCurrentWavesHealthyDispatchFailed()
        {
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            var target = new SubstreamTarget(1, 1, point, () => { });
            var oldDispatch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            int dispatches = 0;
            target.SetRollback(_ => Interlocked.Increment(ref dispatches) == 1
                ? oldDispatch.Task
                : Task.CompletedTask);
            point.Waves.ForStart();

            var oldWave = new RecoveryWave(1, Guid.NewGuid());
            var currentWave = new RecoveryWave(2, Guid.NewGuid());
            var oldNotification = handler.CallFailAndRecover!(oldWave);
            Assert.Equal(1, Volatile.Read(ref dispatches));

            await handler.CallFailAndRecover!(currentWave);
            Assert.Equal(2, Volatile.Read(ref dispatches));
            Assert.True(point.Waves.RestartPending);

            oldDispatch.SetException(new InvalidOperationException("the old dispatch failed late"));
            await Assert.ThrowsAsync<InvalidOperationException>(() => oldNotification);
            Assert.Equal(currentWave, point.Waves.Current);
            Assert.False(point.Waves.TakeFailedRestartDispatch(out _));
        }

        /// <summary>
        /// A held handshake retries a failed restart from a notification
        /// </summary>
        [Fact]
        public async Task AFailedRestartFromAPeersNotificationIsDispatchedAgainByAHeldHandshake()
        {
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            var target = new SubstreamTarget(1, 1, point, () => { });
            int dispatches = 0;
            target.SetRollback(_ =>
            {
                if (Interlocked.Increment(ref dispatches) == 1)
                {
                    throw new InvalidOperationException("the first dispatch fails");
                }
                return Task.CompletedTask;
            });
            point.Waves.ForStart();

            var wave = new RecoveryWave(1, Guid.NewGuid());
            await Assert.ThrowsAsync<InvalidOperationException>(() => handler.CallFailAndRecover!(wave));
            Assert.Equal(1, Volatile.Read(ref dispatches));

            Assert.True((await handler.InitializeFromTarget!(1, 1, false, wave)).NotStarted);
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (Volatile.Read(ref dispatches) < 2 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            Assert.Equal(2, Volatile.Read(ref dispatches));
        }

        /// <summary>
        /// A retry for a wave entered midway records its own failure
        /// </summary>
        [Fact]
        public async Task ARetryTakenForAWaveEnteredMidHandshakeRecordsItsOwnFailure()
        {
            var logger = new HookLogger();
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(logger, "substream_0", "substream_1", handler);
            var target = new SubstreamTarget(1, 1, point, () => { });
            int dispatches = 0;
            target.SetRollback(_ =>
            {
                if (Interlocked.Increment(ref dispatches) == 1)
                {
                    throw new InvalidOperationException("the retried dispatch fails too");
                }
                return Task.CompletedTask;
            });
            point.Waves.ForStart();
            var answered = new RecoveryWave(1, Guid.NewGuid());
            Assert.True(point.Waves.TryEnter(answered));

            // Mid handshake, enter a wave whose restart failed to dispatch
            var entered = new RecoveryWave(2, Guid.NewGuid());
            logger.OnMessage = message =>
            {
                if (message.Contains("has not restarted into", StringComparison.Ordinal))
                {
                    logger.OnMessage = null;
                    Assert.True(point.Waves.TryEnter(entered));
                    point.Waves.RestartDispatchFailed(entered);
                }
            };
            Assert.True((await handler.InitializeFromTarget!(1, 1, false, answered)).NotStarted);
            Assert.Null(logger.OnMessage);

            // The retry fails so a later handshake must dispatch again
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (Volatile.Read(ref dispatches) < 2 && DateTime.UtcNow < deadline)
            {
                Assert.True((await handler.InitializeFromTarget!(1, 1, false, entered)).NotStarted);
                await Task.Delay(10);
            }
            Assert.Equal(2, Volatile.Read(ref dispatches));
        }

        /// <summary>
        /// A fail and recover from the other substream to a target that exists but has not
        /// been initialized yet must complete without an error, the stream has no committed
        /// work past the recovery point.
        /// </summary>
        [Fact]
        public async Task FailAndRecoverBeforeTargetInitializedIsIgnored()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            // The target registers itself with the communication point in its constructor,
            // like the exchange operator creates it, but it is never initialized.
            var target = new SubstreamTarget(1, 1, communicationPoint, () => { });

            Assert.NotNull(handler.CallFailAndRecover);
            await handler.CallFailAndRecover!(new RecoveryWave(5, Guid.NewGuid()));
        }

        /// <summary>
        /// Notifying the other substream about a failure is fire and forget with one
        /// notification per recovery point in flight, several operators failing at once must
        /// not send one notification each.
        /// </summary>
        [Fact]
        public async Task NotifyFailAndRecoverCollapsesConcurrentNotifications()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);

            for (int i = 0; i < 10; i++)
            {
                communicationPoint.NotifyFailAndRecover();
            }

            // The notifications run fire-and-forget on the thread pool. Wait until the send
            // count stops changing before asserting: sampling right after the first send would
            // let a non-collapsing implementation (which schedules all ten) pass, since its
            // later sends have not reached the handler yet.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            int lastCount = -1;
            while (DateTime.UtcNow < deadline)
            {
                await Task.Delay(100);
                int current = handler.SendFailAndRecoverCalls;
                if (current > 0 && current == lastCount)
                {
                    break;
                }
                lastCount = current;
            }
            Assert.True(handler.SendFailAndRecoverCalls >= 1, "The fail and recover notification was never sent");
            Assert.True(handler.SendFailAndRecoverCalls < 10, $"Expected the concurrent notifications to collapse, got {handler.SendFailAndRecoverCalls} sends");
        }

        private class ThrowOnceHandler : ISubstreamCommunicationHandler
        {
            public int FetchDataCalls;

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                if (Interlocked.Increment(ref FetchDataCalls) == 1)
                {
                    throw new InvalidOperationException("Transient fetch failure");
                }
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<RecoveryWave, Task> callFailAndRecover,
                Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                return Task.CompletedTask;
            }

            public void InitializeDurabilityClaims(Func<long, int, long, RecoveryWave, long, long, bool, Task> callReceiveDurabilityClaim)
            {
            }

            public Task SendDurabilityClaim(long version, int radius, long initVersion, RecoveryWave wave, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken) => Task.CompletedTask;

            public Task SendFailAndRecover(RecoveryWave wave)
            {
                return Task.CompletedTask;
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, RecoveryWave wave, CancellationToken cancellationToken)
            {
                return Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
            }
        }

        /// <summary>
        /// A fetch error ends the fetch loop and starts a fail and recover; when that
        /// recovery cannot run (or throws), the stall watchdog must restart the loop for
        /// the remaining subscribers, a dead loop with live subscribers otherwise starves
        /// the stream silently forever.
        /// </summary>
        [Fact]
        public async Task DeadFetchLoopWithSubscribersIsRestartedByTheStallWatchdog()
        {
            var originalInterval = SubstreamCommunicationPoint.StallCheckInterval;
            SubstreamCommunicationPoint.StallCheckInterval = TimeSpan.FromMilliseconds(100);
            try
            {
                var handler = new ThrowOnceHandler();
                var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);

                // The first fetch throws, ending the loop; no read operator is registered
                // so the started recovery has nothing to do and the loop stays dead.
                communicationPoint.Subscribe(1, ev => Task.CompletedTask);

                var deadline = DateTime.UtcNow.AddSeconds(10);
                while (Volatile.Read(ref handler.FetchDataCalls) < 2 && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(50);
                }
                Assert.True(Volatile.Read(ref handler.FetchDataCalls) >= 2, "The fetch loop was never restarted after it died with live subscribers");

                communicationPoint.Unsubscribe(1);
            }
            finally
            {
                SubstreamCommunicationPoint.StallCheckInterval = originalInterval;
            }
        }
    }
}
