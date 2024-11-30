using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FlowtideDotNet.ComputeTests.SourceGenerator.Internal
{
    class ErrorStrategy : DefaultErrorStrategy
    {
        public override void ReportError(Parser recognizer, RecognitionException e)
        {
            NotifyErrorListeners(recognizer, e.Message, e);
            //base.ReportError(recognizer, e);
        }
    }
    class ErrorReporter : IAntlrErrorListener<IToken>
    {
        private readonly string path;
        private readonly SourceProductionContext sourceContext;

        public bool ErrorReported { get; private set; } 

        public ErrorReporter(string path, SourceProductionContext sourceContext)
        {
            this.path = path;
            this.sourceContext = sourceContext;
        }

        public void SyntaxError(TextWriter output, IRecognizer recognizer, IToken offendingSymbol, int line, int charPositionInLine, string msg, RecognitionException e)
        {
            ErrorReported = true;
            sourceContext.ReportDiagnostic(Diagnostic.Create(
                "TESTGEN001", 
                "Test", 
                new TestLocalizableString(msg), 
                DiagnosticSeverity.Error, 
                DiagnosticSeverity.Error, 
                true, 
                0,
                location: Location.Create(path, new Microsoft.CodeAnalysis.Text.TextSpan(), new Microsoft.CodeAnalysis.Text.LinePositionSpan(new LinePosition(line - 1, charPositionInLine), new LinePosition(line - 1, charPositionInLine)))
                ));
        }
    }
}
