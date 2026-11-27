using System.Reflection;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class HostedInitialFlagTests
{
    private sealed class ObservedState(IObjectState<bool> inner, SemaphoreSlim checkpointLock) : IObjectState<bool>
    {
        public int WritesWhileLocked { get; private set; }

        public bool Value
        {
            get => inner.Value;
            set
            {
                Assert.Equal(0, checkpointLock.CurrentCount);
                WritesWhileLocked++;
                inner.Value = value;
            }
        }

        public ValueTask Commit() => inner.Commit();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task HostedInitialFlagIsWrittenUnderTheLoadLock(bool hasRows, bool fails)
    {
        using var fixture = new RowStoreHostRegressionTests();
        var fixtureType = typeof(RowStoreHostRegressionTests);
        var sourceType = fixtureType.GetNestedType("Source", BindingFlags.NonPublic)!;
        var source = (ColumnBatchReadBaseOperator)Activator.CreateInstance(sourceType, [true])!;
        await fixture.InitializeOperator(source);
        var targetType = fixtureType.GetNestedType("ImmediateTarget", BindingFlags.NonPublic)!;
        var target = Activator.CreateInstance(targetType)!;
        using var output = (IngressOutput<StreamEventBatch>)fixtureType.GetMethod("Output", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [source, target])!;
        var ingressState = (IngressState<StreamEventBatch>)typeof(IngressOutput<StreamEventBatch>)
            .GetField("_ingressState", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(output)!;
        var field = typeof(ColumnBatchReadBaseOperator).GetField("_initialSent", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var state = new ObservedState((IObjectState<bool>)field.GetValue(source)!, ingressState._checkpointLock!);
        field.SetValue(source, state);
        if (hasRows)
        {
            var batch = (EventBatchWeighted)fixtureType.GetMethod("Batch", BindingFlags.Static | BindingFlags.NonPublic)!
                .Invoke(null, [1L, "loaded", false, 1])!;
            var batches = (Queue<EventBatchWeighted>)sourceType.GetProperty("Batches")!.GetValue(source)!;
            batches.Enqueue(batch);
        }
        sourceType.GetProperty("FailAfterBatches")!.SetValue(source, fails);
        try
        {
            var load = (Task)sourceType.GetMethod("RunFull")!.Invoke(source, [output])!;
            if (fails)
            {
                var error = await Assert.ThrowsAsync<InvalidOperationException>(() => load);
                Assert.Equal("the source failed", error.Message);
                Assert.False(state.Value);
                Assert.Equal(0, state.WritesWhileLocked);
            }
            else
            {
                await load;
                Assert.True(state.Value);
                Assert.Equal(1, state.WritesWhileLocked);
            }
            Assert.Equal(1, ingressState._checkpointLock!.CurrentCount);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }
}
