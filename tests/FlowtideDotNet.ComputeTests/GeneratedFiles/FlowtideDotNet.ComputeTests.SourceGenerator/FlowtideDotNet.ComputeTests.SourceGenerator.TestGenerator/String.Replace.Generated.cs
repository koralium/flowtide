using System;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;

namespace String
{
    public class Replace
    {
        public static IEnumerable<object[]> GetDataForTest0()
        {
            yield return new object[] { "replace", new IDataValue[] { new StringValue("abcabcabc"), new StringValue("bc"), new StringValue("dd") }, new StringValue("addaddadd") };
            yield return new object[] { "replace", new IDataValue[] { new StringValue("abcabcabc"), new StringValue(" "), new StringValue("dd") }, new StringValue("abcabcabc") };
            yield return new object[] { "replace", new IDataValue[] { new StringValue("abc def ghi"), new StringValue(" "), new StringValue(",") }, new StringValue("abc,def,ghi") };
        }

        [Theory(DisplayName = "basic: Basic examples without any special cases")]
        [MemberData(nameof(GetDataForTest0))]
        public void Test0(string functionName, IDataValue[] arguments, IDataValue expected)
        {
            FunctionsRegister register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);
            var expressionList = new List<Expression>();
            for (int argIndex = 0; argIndex < arguments.Length; argIndex++)
            {
                expressionList.Add(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = argIndex
                    }
                });
            }
            var compiledMethod = ColumnProjectCompiler.Compile(
                new ScalarFunction()
                {
                    ExtensionName = functionName,
                    ExtensionUri = "/functions_string.yaml",
                    Arguments = expressionList
                }
            , register);
            Column[] columns = new Column[arguments.Length];
            for (int i = 0; i < arguments.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
                columns[i].Add(arguments[i]);
            }
            Column resultColumn = Column.Create(GlobalMemoryManager.Instance);
            compiledMethod(new EventBatchData(columns), 0, resultColumn);
            var actual = resultColumn.GetValueAt(0, default);
            Assert.Equal(expected, actual, (x, y) => DataValueComparer.CompareTo(x, y) == 0);
        }

        public static IEnumerable<object[]> GetDataForTest1()
        {
            yield return new object[] { "replace", new IDataValue[] { new StringValue("abcd"), NullValue.Instance, new StringValue(",") }, NullValue.Instance };
            yield return new object[] { "replace", new IDataValue[] { new StringValue("abcd"), new StringValue(" "), NullValue.Instance }, NullValue.Instance };
            yield return new object[] { "replace", new IDataValue[] { NullValue.Instance, new StringValue(" "), new StringValue(",") }, NullValue.Instance };
        }

        [Theory(DisplayName = "null_input: Examples with null as input")]
        [MemberData(nameof(GetDataForTest1))]
        public void Test1(string functionName, IDataValue[] arguments, IDataValue expected)
        {
            FunctionsRegister register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);
            var expressionList = new List<Expression>();
            for (int argIndex = 0; argIndex < arguments.Length; argIndex++)
            {
                expressionList.Add(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = argIndex
                    }
                });
            }
            var compiledMethod = ColumnProjectCompiler.Compile(
                new ScalarFunction()
                {
                    ExtensionName = functionName,
                    ExtensionUri = "/functions_string.yaml",
                    Arguments = expressionList
                }
            , register);
            Column[] columns = new Column[arguments.Length];
            for (int i = 0; i < arguments.Length; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
                columns[i].Add(arguments[i]);
            }
            Column resultColumn = Column.Create(GlobalMemoryManager.Instance);
            compiledMethod(new EventBatchData(columns), 0, resultColumn);
            var actual = resultColumn.GetValueAt(0, default);
            Assert.Equal(expected, actual, (x, y) => DataValueComparer.CompareTo(x, y) == 0);
        }
    }
}
