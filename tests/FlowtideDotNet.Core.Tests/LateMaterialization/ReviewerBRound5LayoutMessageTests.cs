using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerBRound5LayoutMessageTests
{
    [Fact]
    public async Task WidthOnlyLayoutMismatchReportsStoredAndRequestedWidths()
    {
        using var stream = await RowHeapTests.Open();
        var options = new RowStoreOptions { Mode = RowStoreMode.Keyed };
        using (var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(3, [0], [1], [2]), options, GlobalMemoryManager.Instance))
        {
            await host.BeginCheckpointAsync();
            await host.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
        }

        await stream.Manager.InitializeAsync();
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
                new RowStoreHostLayout(4, [0], [1], [2]), options, GlobalMemoryManager.Instance));

        Assert.Equal(
            "Row store 'rows' is stored in mode Keyed with 3 columns, key columns [0], kept columns [1] and stored columns [2] at output positions [], the stream asks for mode Keyed with 4 columns, [0], [1] and [2] at [].",
            exception.Message);
    }
}
