using System.Reflection;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Core.Tests.Operators.Normalization;
using FlowtideDotNet.Storage.DataStructures;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class HostedNormalizationDrainTests
{
    [Fact]
    public async Task HostedNormalizationDrainReturnsTheReceivedBatchRent()
    {
        using var fixture = new HostedNormalizationTests();
        var fixtureType = typeof(HostedNormalizationTests);
        var op = (ColumnNormalizationOperator)fixtureType.GetMethod("Operator", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(fixture, ["2"])!;
        var output = new BufferBlock<IStreamEvent>();
        op.LinkTo(output);
        await fixture.InitializeOperator(op);
        var input = (StreamMessage<StreamEventBatch>)fixtureType.GetMethod("Message", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [new (int weight, long key, long kept, string text)[] { (1, 1, 10, "a") }])!;
        Assert.True(await op.SendAsync(input));
        var received = Assert.IsType<StreamMessage<StreamEventBatch>>(await output.ReceiveAsync(TimeSpan.FromSeconds(30)));
        var data = received.Data.Data;
        var counter = typeof(PrimitiveList<int>).GetField("_rentCounter", BindingFlags.Instance | BindingFlags.NonPublic)!;
        Assert.Equal(1, (int)counter.GetValue(data.Weights)!);
        Assert.True(output.Post(received));
        try
        {
            var drain = (Task<(List<string> rows, Dictionary<string, ulong> references)>)fixtureType
                .GetMethod("Drain", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(fixture, [op, output, 1L])!;
            await drain;

            Assert.Equal(0, (int)counter.GetValue(data.Weights)!);
        }
        finally
        {
            if ((int)counter.GetValue(data.Weights)! > 0)
            {
                data.Return();
            }
            await op.DisposeAsync();
        }
    }
}
