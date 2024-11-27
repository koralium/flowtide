using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;

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

        [Fact]
        public void Test2()
        {
            FunctionsRegister register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);

            register.TryGetColumnScalarFunction("/functions_string.yaml", "upper", out var func);

            var compiledMethod = ColumnProjectCompiler.Compile(
                new Substrait.Expressions.ScalarFunction()
                {
                    Arguments = new List<Substrait.Expressions.Expression>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = 0
                            }
                        }
                    },
                    ExtensionName = "upper",
                    ExtensionUri = "/functions_string.yaml"
                }, register);

            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new StringValue("abc"));
            Column resultColumn = Column.Create(GlobalMemoryManager.Instance);
            compiledMethod(new EventBatchData([column]), 0, resultColumn);

            var res = resultColumn.GetValueAt(0, default);
        }
    }
}