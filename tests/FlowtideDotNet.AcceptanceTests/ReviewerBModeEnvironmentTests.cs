using FlowtideDotNet.AcceptanceTests.Internal;

namespace FlowtideDotNet.AcceptanceTests;

[CollectionDefinition("ReviewerBModeEnvironment", DisableParallelization = true)]
public sealed class ReviewerBModeEnvironmentCollection
{
}

[Collection("ReviewerBModeEnvironment")]
public class ReviewerBModeEnvironmentTests
{
    [Theory]
    [InlineData("99")]
    [InlineData("Force_On")]
    [InlineData("Auto,ForceOn")]
    public async Task AnInvalidSuiteModeIsRefused(string mode)
    {
        const string variable = "FLOWTIDE_TEST_LATE_MATERIALIZATION";
        var original = Environment.GetEnvironmentVariable(variable);
        try
        {
            Environment.SetEnvironmentVariable(variable, mode);
            await using var stream = new FlowtideTestStream("ReviewerBInvalidMode/" + mode);
            stream.Generate(1);
            await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
                await stream.StartStream("INSERT INTO output SELECT userkey, firstName FROM users"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, original);
        }
    }
}
