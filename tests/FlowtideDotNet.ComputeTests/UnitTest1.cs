namespace FlowtideDotNet.ComputeTests
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            string path = AppDomain.CurrentDomain.BaseDirectory;

            var splitByBin = path.Split("bin");

            var testFileLoc = Path.Combine(splitByBin[0], "./Substrait/cases/string/substring.test");
            var testContent = File.ReadAllText(testFileLoc);

            var parser = new TestCaseParser();
            parser.Parse(testContent);
        }
    }
}