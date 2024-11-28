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

using FlowtideDotNet.ComputeTests.Internal.Tests;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;

namespace FlowtideDotNet.ComputeTests.SourceGenerator
{
    /// <summary>
    /// Generator that creates test classes for each test file
    /// </summary>
    [Generator]
    public class TestGenerator : IIncrementalGenerator
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

            var content = text.GetText();
            var textContent = content.ToString();

            var testDocument = new TestCaseParser().Parse(textContent);

            if (testDocument.ScalarTestGroups != null)
            {
                ExecuteScalar(text, context, testDocument, folderName, className);
            }
            if (testDocument.AggregateTestGroups != null)
            {
                ExecuteAggregate(text, context, testDocument, folderName, className);
            }
        }

        private void ExecuteAggregate(AdditionalText text, SourceProductionContext context, TestDocument testDocument, string folderName, string className)
        {
            OutputWriter testClassBuilder = new OutputWriter();

            testClassBuilder.AppendLine("using System;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute.Internal;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Substrait.Expressions;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute.Columnar;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Storage.Memory;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore.Comparers;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore.DataValues;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Storage.StateManager;");
            testClassBuilder.AppendLine("using Microsoft.Extensions.Logging.Abstractions;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.Operators.Aggregate.Column;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore.TreeStorage;");

            testClassBuilder.AppendLine();

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

            testClassBuilder.AppendLine($"namespace {folderName}");
            testClassBuilder.StartCurly();

            // Make class name first letter uppercase
            className = char.ToUpper(className[0]) + className.Substring(1);

            testClassBuilder.AppendLine($"public class {className}");

            testClassBuilder.StartCurly();

            int testIndex = 0;
            // Start of tests
            foreach(var aggTestGroup in testDocument.AggregateTestGroups)
            {
                // Start test case data
                testClassBuilder.AppendLine($"public static IEnumerable<object[]> GetDataForTest{testIndex}()");
                testClassBuilder.StartCurly();

                foreach (var test in aggTestGroup.TestCases)
                {
                    List<string> columnsCreation = new List<string>();

                    foreach(var column in test.Columns)
                    {
                        columnsCreation.Add($"new Column(GlobalMemoryManager.Instance) {{ {string.Join(", ", column)} }}");
                    }

                    List<string> argumentValues = [
                        $"\"{test.FunctionName}\"",
                        $"new EventBatchData([{string.Join(", ", columnsCreation)}])"];

                    argumentValues.Add(test.ExpectedResult.ExpectedValue);

                    var optionsList = $"new SortedList<string, string>() {{ {string.Join(", ", test.Options.Select(x => $"{{ \"{x.Key}\", \"{x.Value}\" }}"))} }}";
                    argumentValues.Add(optionsList);

                    var argList = string.Join(", ", argumentValues);

                    testClassBuilder.AppendLine($"yield return new object[] {{ {argList} }};");
                }

                testClassBuilder.EndCurly();
                // End of test case data

                testClassBuilder.AppendLine();
                testClassBuilder.AppendLine($"[Theory(DisplayName = \"{aggTestGroup.Description}\")]");
                testClassBuilder.AppendLine($"[MemberData(nameof(GetDataForTest{testIndex}))]");

                List<string> argNames = new()
                {
                    "string functionName",
                    "EventBatchData rowBatch",
                    "IDataValue expected",
                    "SortedList<string, string> options"
                };

                testClassBuilder.AppendLine($"public async Task Test{testIndex}({string.Join(", ", argNames)})");
                testClassBuilder.StartCurly();
                // start test content

                testClassBuilder.AppendLine("FunctionsRegister register = new FunctionsRegister();");
                testClassBuilder.AppendLine("BuiltinFunctions.RegisterFunctions(register);");

                testClassBuilder.AppendLine("var expressionList = new List<Expression>();");

                testClassBuilder.AppendLine("for (int argIndex = 0; argIndex < rowBatch.Columns.Count; argIndex++)");
                testClassBuilder.StartCurly();

                testClassBuilder.AppendLine("expressionList.Add(new DirectFieldReference()");
                testClassBuilder.StartCurly();
                testClassBuilder.AppendLine("ReferenceSegment = new StructReferenceSegment()");
                testClassBuilder.StartCurly();
                testClassBuilder.AppendLine($"Field = argIndex");
                testClassBuilder.EndCurly();

                testClassBuilder.EndCurly(");");
                testClassBuilder.EndCurly();

                testClassBuilder.AppendLine("AggregateFunction aggregateFunction = new AggregateFunction()");
                testClassBuilder.StartCurly();

                testClassBuilder.AppendLine($"ExtensionName = functionName,");
                testClassBuilder.AppendLine($"ExtensionUri = \"{includePath}\",");
                testClassBuilder.AppendLine($"Options = options,");
                testClassBuilder.AppendLine("Arguments = expressionList");
                testClassBuilder.EndCurly(";");


                // Create the state manager for aggregations that require trees
                testClassBuilder.AppendLine("using StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions(), NullLogger.Instance, new System.Diagnostics.Metrics.Meter(\"\"), \"\");");
                testClassBuilder.AppendLine("await stateManager.InitializeAsync();");
                testClassBuilder.AppendLine("var stateClient = stateManager.GetOrCreateClient(\"a\");");

                // Compile
                testClassBuilder.AppendLine("var compileResult = await ColumnMeasureCompiler.CompileMeasure(0, stateClient, aggregateFunction, register, GlobalMemoryManager.Instance);");

                // Group key batch
                testClassBuilder.AppendLine("IColumn[] groupingBatchColumns = new IColumn[0];");
                testClassBuilder.AppendLine("var groupBatch = new EventBatchData(groupingBatchColumns);");
                testClassBuilder.AppendLine("var groupingKey = new ColumnRowReference() { referenceBatch = groupBatch, RowIndex = 0 };");

                // State column
                testClassBuilder.AppendLine("Column stateColumn = Column.Create(GlobalMemoryManager.Instance);");
                testClassBuilder.AppendLine("stateColumn.Add(NullValue.Instance);");
                testClassBuilder.AppendLine("var stateColumnRef = new ColumnReference(stateColumn, 0, default);");

                // Loop through the rows
                testClassBuilder.AppendLine("for (int i = 0; i < rowBatch.Count; i++)");
                testClassBuilder.StartCurly();
                testClassBuilder.AppendLine("await compileResult.Compute(groupingKey, rowBatch, i, stateColumnRef, 1);");
                testClassBuilder.EndCurly();

                // Create the output column
                testClassBuilder.AppendLine("Column outputColumn = Column.Create(GlobalMemoryManager.Instance);");

                // Get the value
                testClassBuilder.AppendLine("await compileResult.GetValue(groupingKey, stateColumnRef, outputColumn);");

                // Get the actual value
                testClassBuilder.AppendLine("var actual = outputColumn.GetValueAt(0, default);");

                // Assert
                testClassBuilder.AppendLine("Assert.Equal(expected, actual, (x, y) => DataValueComparer.CompareTo(x, y) == 0);");

                // end test content
                testClassBuilder.EndCurly();

                testIndex++;
            }


            testClassBuilder.EndCurly();
            testClassBuilder.EndCurly();

            context.AddSource($"{folderName}.{className}.Generated.cs", testClassBuilder.ToString());
        }

        private void ExecuteScalar(AdditionalText text, SourceProductionContext context, TestDocument testDocument, string folderName, string className)
        {
            OutputWriter testClassBuilder = new OutputWriter();
            testClassBuilder.AppendLine("using System;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute.Internal;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Substrait.Expressions;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.Compute.Columnar;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Storage.Memory;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore.Comparers;");
            testClassBuilder.AppendLine("using FlowtideDotNet.Core.ColumnStore.DataValues;");

            testClassBuilder.AppendLine();

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

            testClassBuilder.AppendLine($"namespace {folderName}");
            testClassBuilder.StartCurly();

            // Make class name first letter uppercase
            className = char.ToUpper(className[0]) + className.Substring(1);

            testClassBuilder.AppendLine($"public class {className}");

            testClassBuilder.StartCurly();

            int i = 0;
            foreach (var testGroup in testDocument.ScalarTestGroups)
            {
                if (i > 0)
                {
                    testClassBuilder.AppendLine();
                }

                testClassBuilder.AppendLine($"public static IEnumerable<object[]> GetDataForTest{i}()");
                testClassBuilder.StartCurly();

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

                    
                    var optionsList = $"new SortedList<string, string>() {{ {string.Join(", ", test.Options.Select(x => $"{{ \"{x.Key}\", \"{x.Value}\" }}"))} }}";
                    argumentValues.Add(optionsList);

                    var argList = string.Join(", ", argumentValues);
                    testClassBuilder.AppendLine($"yield return new object[] {{ {argList} }};");
                }

                testClassBuilder.EndCurly();

                testClassBuilder.AppendLine();

                testClassBuilder.AppendLine($"[Theory(DisplayName = \"{testGroup.Description}\")]");
                testClassBuilder.AppendLine($"[MemberData(nameof(GetDataForTest{i}))]");

                List<string> argNames = new()
                {
                    "string functionName",
                    "IDataValue[] arguments",
                    "IDataValue expected",
                    "SortedList<string, string> options"
                };

                testClassBuilder.AppendLine($"public void Test{i}({string.Join(", ", argNames)})");

                testClassBuilder.StartCurly();

                testClassBuilder.AppendLine("FunctionsRegister register = new FunctionsRegister();");
                testClassBuilder.AppendLine("BuiltinFunctions.RegisterFunctions(register);");

                testClassBuilder.AppendLine("var expressionList = new List<Expression>();");

                testClassBuilder.AppendLine("for (int argIndex = 0; argIndex < arguments.Length; argIndex++)");
                testClassBuilder.StartCurly();

                testClassBuilder.AppendLine("expressionList.Add(new DirectFieldReference()");
                testClassBuilder.StartCurly();
                testClassBuilder.AppendLine("ReferenceSegment = new StructReferenceSegment()");
                testClassBuilder.StartCurly();
                testClassBuilder.AppendLine($"Field = argIndex");
                testClassBuilder.EndCurly();

                testClassBuilder.EndCurly(");");
                testClassBuilder.EndCurly();

                testClassBuilder.AppendLine("var compiledMethod = ColumnProjectCompiler.Compile(");
                testClassBuilder.Indent();
                testClassBuilder.AppendLine("new ScalarFunction()");
                testClassBuilder.StartCurly();
                testClassBuilder.AppendLine($"ExtensionName = functionName,");
                testClassBuilder.AppendLine($"ExtensionUri = \"{includePath}\",");
                testClassBuilder.AppendLine($"Options = options,");
                testClassBuilder.AppendLine("Arguments = expressionList");

                testClassBuilder.EndCurly();
                testClassBuilder.Dedent();

                testClassBuilder.AppendLine(", register);");

                testClassBuilder.AppendLine("Column[] columns = new Column[arguments.Length];");

                testClassBuilder.AppendLine("for (int i = 0; i < arguments.Length; i++)");
                testClassBuilder.StartCurly();
                testClassBuilder.AppendLine("columns[i] = Column.Create(GlobalMemoryManager.Instance);");
                testClassBuilder.AppendLine("columns[i].Add(arguments[i]);");
                testClassBuilder.EndCurly();

                // Create result column
                testClassBuilder.AppendLine("Column resultColumn = Column.Create(GlobalMemoryManager.Instance);");

                // Execute the compiled method
                testClassBuilder.AppendLine("compiledMethod(new EventBatchData(columns), 0, resultColumn);");

                testClassBuilder.AppendLine("var actual = resultColumn.GetValueAt(0, default);");

                testClassBuilder.AppendLine("Assert.Equal(expected, actual, (x, y) => DataValueComparer.CompareTo(x, y) == 0);");

                testClassBuilder.EndCurly();

                i++;
            }

            testClassBuilder.EndCurly();

            testClassBuilder.EndCurly();

            context.AddSource($"{folderName}.{className}.Generated.cs", testClassBuilder.ToString());
        }
    }
}
