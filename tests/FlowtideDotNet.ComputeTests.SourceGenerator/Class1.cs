using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace FlowtideDotNet.ComputeTests.SourceGenerator
{
    [Generator]
    public class Class1 : IIncrementalGenerator
    {
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            IncrementalValuesProvider<AdditionalText> testFiles = context.AdditionalTextsProvider
                .Where(static file => file.Path.EndsWith(".test"));
            context.RegisterSourceOutput(testFiles, (spec, content) =>
            {
                Execute(content, spec);
            });
        }

        private class OutputWriter
        {
            private int indent = 0;
            StringBuilder sb = new StringBuilder();
            public OutputWriter()
            {

            }

            public void AppendLine(string line)
            {
                sb.AppendLine(new string(' ', indent * 4) + line);
            }

            public void Indent()
            {
                indent++;
            }

            public void Dedent()
            {
                indent--;
            }

            public void StartCurly()
            {
                AppendLine("{");
                Indent();
            }

            public void EndCurly(string extra = default)
            {
                Dedent();
                AppendLine("}" + extra);
            }

            public void EndCurlyNoNewLine()
            {
                Dedent();
                sb.Append(new string(' ', indent * 4) + "}");
            }

            public void Append(string text)
            {
                sb.Append(text);
            }

            public void AppendLine()
            {
                sb.AppendLine();
            }

            public void AppendLineNoIndent(string text)
            {
                sb.AppendLine(text);
            }

            public override string ToString()
            {
                return sb.ToString();
            }
        }

            public void Execute(AdditionalText text, SourceProductionContext context)
        {
            var fileName = Path.GetFileName(text.Path);
            var className = Path.GetFileNameWithoutExtension(fileName);

            // Get the last folder name that the file is in
            var folderName = Path.GetFileName(Path.GetDirectoryName(text.Path));

            // Make first character uppercase
            folderName = char.ToUpper(folderName[0]) + folderName.Substring(1);

            OutputWriter calcGeneratedClassBuilder = new OutputWriter();
            calcGeneratedClassBuilder.AppendLine("using System;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute.Internal;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Substrait.Expressions;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute.Columnar;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Storage.Memory;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore.Comparers;");
            calcGeneratedClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore.DataValues;");

            calcGeneratedClassBuilder.AppendLine();

            var content = text.GetText();
            var textContent = content.ToString();

            var testDocument = new TestCaseParser().Parse(textContent);

            var includePaths = testDocument.Header.Include.IncludePaths;

            if (includePaths.Count != 1)
            {
                return;
            }

            var includePath = includePaths[0];

            if (includePath.StartsWith("/extensions"))
            {
                //Remove extensions
                includePath = includePath.Substring(11);
            }

            calcGeneratedClassBuilder.AppendLine($"namespace {folderName}");
            calcGeneratedClassBuilder.StartCurly();

            // Make class name first letter uppercase
            className = char.ToUpper(className[0]) + className.Substring(1);

            calcGeneratedClassBuilder.AppendLine($"public class {className}");

            calcGeneratedClassBuilder.StartCurly();

            int i = 0;
            foreach (var testGroup in testDocument.ScalarTestGroups)
            {
                //int maxArgCount = testGroup.TestCases.Max(test => test.Arguments.Count);

                if (i > 0)
                {
                    calcGeneratedClassBuilder.AppendLine();
                }

                calcGeneratedClassBuilder.AppendLine($"public static IEnumerable<object[]> GetDataForTest{i}()");
                calcGeneratedClassBuilder.StartCurly();

                

                foreach (var test in testGroup.TestCases)
                {
                    List<string> argumentValues = new List<string>
                    {
                        // Add function name as a parameter
                        $"\"{test.FunctionName}\""
                    };
                    var dataList = $"new IDataValue[] {{ {string.Join(", ", test.Arguments)} }}";
                    argumentValues.Add(dataList);

                    argumentValues.Add(test.ExpectedResult.ExpectedValue);
                    var argList = string.Join(", ", argumentValues);
                    calcGeneratedClassBuilder.AppendLine($"yield return new object[] {{ {argList} }};");
                }

                calcGeneratedClassBuilder.EndCurly();

                calcGeneratedClassBuilder.AppendLine();

                calcGeneratedClassBuilder.AppendLine($"[Theory(DisplayName = \"{testGroup.Description}\")]");
                calcGeneratedClassBuilder.AppendLine($"[MemberData(nameof(GetDataForTest{i}))]");

                List<string> argNames = new()
                {
                    "string functionName",
                    "IDataValue[] arguments",
                    "IDataValue expected"
                };
                //for (int j = 0; j < maxArgCount; j++)
                //{
                //    argNames.Add($"IDataValue arg{j}");
                //}

                calcGeneratedClassBuilder.AppendLine($"public void Test{i}({string.Join(", ", argNames)})");

                calcGeneratedClassBuilder.StartCurly();

                calcGeneratedClassBuilder.AppendLine("FunctionsRegister register = new FunctionsRegister();");
                calcGeneratedClassBuilder.AppendLine("BuiltinFunctions.RegisterFunctions(register);");

                calcGeneratedClassBuilder.AppendLine("var expressionList = new List<Expression>();");

                calcGeneratedClassBuilder.AppendLine("for (int argIndex = 0; argIndex < arguments.Length; argIndex++)");
                calcGeneratedClassBuilder.StartCurly();

                calcGeneratedClassBuilder.AppendLine("expressionList.Add(new DirectFieldReference()");
                calcGeneratedClassBuilder.StartCurly();
                calcGeneratedClassBuilder.AppendLine("ReferenceSegment = new StructReferenceSegment()");
                calcGeneratedClassBuilder.StartCurly();
                calcGeneratedClassBuilder.AppendLine($"Field = argIndex");
                calcGeneratedClassBuilder.EndCurly();

                calcGeneratedClassBuilder.EndCurly(");");
                calcGeneratedClassBuilder.EndCurly();

                calcGeneratedClassBuilder.AppendLine("var compiledMethod = ColumnProjectCompiler.Compile(");
                calcGeneratedClassBuilder.Indent();
                calcGeneratedClassBuilder.AppendLine("new ScalarFunction()");
                calcGeneratedClassBuilder.StartCurly();
                calcGeneratedClassBuilder.AppendLine($"ExtensionName = functionName,");
                calcGeneratedClassBuilder.AppendLine($"ExtensionUri = \"{includePath}\",");
                calcGeneratedClassBuilder.AppendLine("Arguments = expressionList");
                //calcGeneratedClassBuilder.StartCurly();
                //for (int j = 0; j < maxArgCount; j++)
                //{
                //    calcGeneratedClassBuilder.AppendLine("new DirectFieldReference()");
                //    calcGeneratedClassBuilder.StartCurly();
                //    calcGeneratedClassBuilder.AppendLine("ReferenceSegment = new StructReferenceSegment()");
                //    calcGeneratedClassBuilder.StartCurly();
                //    calcGeneratedClassBuilder.AppendLine($"Field = {j}");
                //    calcGeneratedClassBuilder.EndCurly();
                //    calcGeneratedClassBuilder.EndCurlyNoNewLine();

                //    if ((j + 1) < maxArgCount)
                //    {
                //        calcGeneratedClassBuilder.AppendLineNoIndent(",");
                //    }
                //    else
                //    {
                //        calcGeneratedClassBuilder.AppendLine();
                //    }

                //}

                //calcGeneratedClassBuilder.EndCurly();
                calcGeneratedClassBuilder.EndCurly();
                calcGeneratedClassBuilder.Dedent();

                calcGeneratedClassBuilder.AppendLine(", register);");

                calcGeneratedClassBuilder.AppendLine("Column[] columns = new Column[arguments.Length];");

                calcGeneratedClassBuilder.AppendLine("for (int i = 0; i < arguments.Length; i++)");
                calcGeneratedClassBuilder.StartCurly();
                calcGeneratedClassBuilder.AppendLine("columns[i] = Column.Create(GlobalMemoryManager.Instance);");
                calcGeneratedClassBuilder.AppendLine("columns[i].Add(arguments[i]);");
                calcGeneratedClassBuilder.EndCurly();

                // Create result column
                calcGeneratedClassBuilder.AppendLine("Column resultColumn = Column.Create(GlobalMemoryManager.Instance);");

                // Execute the compiled method
                calcGeneratedClassBuilder.AppendLine("compiledMethod(new EventBatchData(columns), 0, resultColumn);");

                calcGeneratedClassBuilder.AppendLine("var actual = resultColumn.GetValueAt(0, default);");

                calcGeneratedClassBuilder.AppendLine("Assert.Equal(expected, actual, (x, y) => DataValueComparer.CompareTo(x, y) == 0);");

                calcGeneratedClassBuilder.EndCurly();

                i++;
            }

            calcGeneratedClassBuilder.EndCurly();

            calcGeneratedClassBuilder.EndCurly();

            context.AddSource($"{folderName}.{className}.Generated.cs", calcGeneratedClassBuilder.ToString());
        }
    }
}
