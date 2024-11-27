using Microsoft.CodeAnalysis;
using System;

namespace FlowtideDotNet.ComputeTests.SourceGenerator
{
    public class Class1 : IIncrementalGenerator
    {
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            IncrementalValuesProvider<AdditionalText> testFiles = context.AdditionalTextsProvider
                .Where(static file => file.Path.EndsWith(".test"));
            context.RegisterSourceOutput(testFiles, (spec, content) =>
            {
                
            });
        }
    }
}
