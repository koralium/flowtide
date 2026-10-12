using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Tests.Exchange;

// Serialized with SubstreamDurabilityClaimResendTests, both set the durability resend statics.
[Collection("SubstreamDurabilityResendStatics")]
public class SubstreamDurabilitySenderTests
{
    [Fact]
    public async Task NormalSendsAndRepliesShareTheSameActualOperationBudget()
    {
        var hub = new LocalSubstreamCommunicationHub();
        var handler = new UnsettledHandler(hub.CreateFactory("a").GetCommunicationHandler("b", "a"));
        var peerHandler = hub.CreateFactory("b").GetCommunicationHandler("a", "b");
        var claims = new SubstreamDurabilityCoordinator(NullLogger.Instance, "a", new[] { "b" }, 1);
        var a = new SubstreamCommunicationPoint(NullLogger.Instance, "a", "b", handler, false, claims, Starting());
        var b = new SubstreamCommunicationPoint(NullLogger.Instance, "b", "a", peerHandler, waves: Starting());
        try
        {
            await a.InitializeOperator(0);
            await b.InitializeOperator(0);
            handler.HoldAll = true;

            // No agreement wait or timer loop: these are three ordinary entry points.
            claims.LocalDurable(1);
            claims.ResendTo(a);
            claims.PeerClaim("b", new SubstreamDurabilityClaim(0, 1, 0),
                claims.Wave, claims.Generation, requestReply: true);

            await WaitUntil(() => handler.UnsettledCount == 1);
            Assert.True(handler.UnsettledCount <= 1,
                $"Expected the peer-wide operation budget to cover immediate sends and replies; found {handler.UnsettledCount} actual unsettled transport calls without starting a resend timer.");
        }
        finally
        {
            handler.ReleaseAll();
        }
    }

    [Fact]
    public async Task CancellationAndEpochChangesKeepTheActualOperationSlotOccupied()
    {
        var oldInterval = SubstreamDurabilityCoordinator.ResendInterval;
        var oldAbandon = SubstreamDurabilityCoordinator.ResendAbandonAfter;
        SubstreamDurabilityCoordinator.ResendInterval = TimeSpan.FromMilliseconds(20);
        SubstreamDurabilityCoordinator.ResendAbandonAfter = TimeSpan.FromMilliseconds(60);
        using var cancellation = new CancellationTokenSource();
        UnsettledHandler? handler = null;
        Task? agreement = null;
        try
        {
            var hub = new LocalSubstreamCommunicationHub();
            handler = new UnsettledHandler(hub.CreateFactory("a").GetCommunicationHandler("b", "a"));
            var peerHandler = hub.CreateFactory("b").GetCommunicationHandler("a", "b");
            var claims = new SubstreamDurabilityCoordinator(NullLogger.Instance, "a", new[] { "b" }, 1);
            var a = new SubstreamCommunicationPoint(NullLogger.Instance, "a", "b", handler, false, claims, Starting());
            var b = new SubstreamCommunicationPoint(NullLogger.Instance, "b", "a", peerHandler, waves: Starting());
            await a.InitializeOperator(0);
            await b.InitializeOperator(0);
            claims.LocalDurable(5);
            agreement = claims.WhenAgreed(5, cancellation.Token);
            await WaitUntil(() => handler.UnsettledCount == 1);
            await Task.Delay(200); // Several timer ticks and the cancellation request.
            cancellation.Cancel();
            try { await agreement; } catch (OperationCanceledException) { }
            claims.Invalidate();
            claims.EnterWave(claims.Wave);
            claims.LocalInit(1);
            claims.LocalDurable(2);
            claims.PeerEpochChanged("b");
            claims.ResendTo(a);
            await Task.Delay(100);

            Assert.True(handler.UnsettledCount <= 1,
                $"Expected at most one outstanding request to this peer; found {handler.UnsettledCount} actual unsettled transport calls.");
        }
        finally
        {
            cancellation.Cancel();
            handler?.ReleaseAll();
            if (agreement != null)
            {
                try { await agreement; }
                catch (OperationCanceledException) { }
            }
            SubstreamDurabilityCoordinator.ResendInterval = oldInterval;
            SubstreamDurabilityCoordinator.ResendAbandonAfter = oldAbandon;
        }
    }

    [Fact]
    public async Task CoalescingDeliversTheWholeCurrentFrontierAfterSettlement()
    {
        var hub = new LocalSubstreamCommunicationHub();
        var handler = new UnsettledHandler(hub.CreateFactory("a").GetCommunicationHandler("b", "a"));
        var claims = new SubstreamDurabilityCoordinator(NullLogger.Instance, "a", new[] { "b" }, 2);
        var a = new SubstreamCommunicationPoint(NullLogger.Instance, "a", "b", handler, false, claims, Starting());
        var b = new SubstreamCommunicationPoint(NullLogger.Instance, "b", "a",
            hub.CreateFactory("b").GetCommunicationHandler("a", "b"), waves: Starting());
        try
        {
            await a.InitializeOperator(0);
            await b.InitializeOperator(0);
            handler.HoldAll = true;
            claims.LocalDurable(10);
            await WaitUntil(() => handler.UnsettledCount == 1);
            claims.PeerClaim("b", new(0, 9, 0), claims.Wave, claims.Generation, true);
            claims.PeerClaim("b", new(1, 8, 0), claims.Wave, claims.Generation, true);
            handler.ReleaseAll();
            await WaitUntil(() => handler.HasDelivered(0, 10) && handler.HasDelivered(1, 9) && handler.HasDelivered(2, 8));
        }
        finally { handler.ReleaseAll(); }
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition()) await Task.Delay(10, timeout.Token);
    }

    private static SubstreamRecoveryWaves Starting()
    {
        var waves = new SubstreamRecoveryWaves();
        waves.ForStart();
        return waves;
    }

    private sealed class UnsettledHandler(ISubstreamCommunicationHandler inner) : ISubstreamCommunicationHandler
    {
        private readonly List<TaskCompletionSource> operations = new();
        private bool releasing;
        private readonly HashSet<(int Radius, long Version)> delivered = new();
        public bool HasDelivered(int radius, long version) { lock (operations) return delivered.Contains((radius, version)); }
        public bool HoldAll { get; set; }

        public int UnsettledCount
        {
            get { lock (operations) return operations.Count(t => !t.Task.IsCompleted); }
        }

        public void ReleaseAll()
        {
            lock (operations)
            {
                releasing = true;
                foreach (var operation in operations) operation.TrySetResult();
            }
        }

        public void Initialize(
            Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
            Func<RecoveryWave, Task> callFailAndRecover,
            Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>> initializeFromTarget,
            Func<long, long, bool, Task> callRecieveCheckpointDone)
            => inner.Initialize(getDataFunction, callFailAndRecover, initializeFromTarget, callRecieveCheckpointDone);

        public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            => inner.FetchData(targetIds, numberOfEvents, cancellationToken);

        public Task SendFailAndRecover(RecoveryWave wave) => inner.SendFailAndRecover(wave);

        public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, RecoveryWave wave, CancellationToken cancellationToken)
            => inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, wave, cancellationToken);

        public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            => inner.SendCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);

        public void InitializeDurabilityClaims(Func<long, int, long, RecoveryWave, long, long, bool, Task> callReceiveDurabilityClaim)
            => inner.InitializeDurabilityClaims(callReceiveDurabilityClaim);

        public Task SendDurabilityClaim(long version, int radius, long initVersion, RecoveryWave wave, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken)
        {
            if (!HoldAll && !requestReply)
                return inner.SendDurabilityClaim(version, radius, initVersion, wave, senderCheckpointEpoch, targetCheckpointEpoch, false, cancellationToken);
            lock (operations)
            {
                if (releasing)
                {
                    delivered.Add((radius, version));
                    return Task.CompletedTask;
                }
                var operation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                operations.Add(operation);
                return operation.Task; // Deliberately ignores cancellation.
            }
        }
    }
}
