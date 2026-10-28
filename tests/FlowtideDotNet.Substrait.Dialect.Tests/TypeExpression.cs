// Licensed under the Apache License, Version 2.0.
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using FlowtideDotNet.Substrait.Dialect.Tests.Grammar;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

internal sealed record TypeReference(string? Alias, string Name);
internal sealed record ParsedType(string ShortName, IReadOnlyList<TypeReference> References);

internal static class TypeExpression
{
    public static ParsedType Parse(string expression)
    {
        var errors = new Errors();
        var lexer = new SubstraitTypeLexer(new AntlrInputStream(expression));
        lexer.RemoveErrorListeners();
        lexer.AddErrorListener(errors);
        var parser = new SubstraitTypeParser(new CommonTokenStream(lexer));
        parser.RemoveErrorListeners();
        parser.AddErrorListener(errors);
        var tree = parser.startRule();
        if (errors.Messages.Count != 0)
            throw new InvalidDataException($"Invalid Substrait type '{expression}': {string.Join("; ", errors.Messages)}");

        var nodes = Descendants(tree).ToArray();
        var rootType = nodes.OfType<SubstraitTypeParser.TypeDefContext>().FirstOrDefault();
        var userType = rootType?.parameterizedType() as SubstraitTypeParser.UserDefinedContext;
        var name = userType != null ? "u!" + userType.Identifier().Last().GetText()
            : rootType?.Start.Text.ToLowerInvariant() ?? expression.Trim().ToLowerInvariant();
        var shortName = name switch
        {
            "boolean" => "bool", "string" => "str", "binary" => "vbin", "decimal" => "dec",
            "fixedchar" => "fchar", "varchar" => "vchar", "fixedbinary" => "fbin",
            "interval_year" => "iyear", "interval_day" => "iday", "interval_compound" => "icompound",
            "precision_time" => "pt", "precision_timestamp" => "pts", "precision_timestamp_tz" => "ptstz",
            _ when System.Text.RegularExpressions.Regex.IsMatch(name, "^any[0-9]*$") => "any",
            _ => name
        };
        var references = nodes.OfType<SubstraitTypeParser.UserDefinedContext>()
            .Select(node => new TypeReference(node.dependencyAlias?.Text, node.Identifier().Last().GetText())).ToArray();
        return new ParsedType(shortName, references);
    }

    private static IEnumerable<IParseTree> Descendants(IParseTree tree)
    {
        yield return tree;
        for (var i = 0; i < tree.ChildCount; i++)
            foreach (var child in Descendants(tree.GetChild(i))) yield return child;
    }

    private sealed class Errors : BaseErrorListener, IAntlrErrorListener<int>
    {
        public List<string> Messages { get; } = [];
        public override void SyntaxError(TextWriter output, IRecognizer recognizer, IToken offendingSymbol,
            int line, int charPositionInLine, string msg, RecognitionException e)
            => Messages.Add($"{line}:{charPositionInLine + 1} {msg}");
        void IAntlrErrorListener<int>.SyntaxError(TextWriter output, IRecognizer recognizer, int offendingSymbol,
            int line, int charPositionInLine, string msg, RecognitionException e)
            => Messages.Add($"{line}:{charPositionInLine + 1} {msg}");
    }
}
