# Flowtide Substrait catalog

`dialect.yaml` describes the supported expressions, function bindings, relations and types.
`extensions/` contains Flowtide definitions grouped by function family. Standard function
URNs resolve against upstream Substrait definitions; Flowtide function metadata records
the legacy URI/name bindings used by the runtime.

## Validation

Run from the repository root:

```sh
dotnet test tests/FlowtideDotNet.Substrait.Dialect.Tests/FlowtideDotNet.Substrait.Dialect.Tests.csproj --configuration Release --framework net10.0
```

The test project also supports `net8.0`. The `.NET` GitHub Actions workflow runs it in
the independent `validate-substrait` job on pushes and pull requests to `main`.

Validation checks:

- YAML and the official dialect and Simple Extensions JSON schemas.
- Extension dependencies, function names, argument signatures, selected options and variadic bounds.
- Type expressions using Substrait's official grammar, including user-defined type references.
- Flowtide's custom table-function metadata, output column counts and constant arguments.
- Coverage of registered scalar, aggregate, window and table functions, runtime relation
  implementations and native type models. The SQL-only `gettimestamp` intrinsic has a
  separate planner binding check.

Errors include the YAML file and JSON pointer when applicable. These are structural and
coverage checks: changes to runtime semantics, argument restrictions or import/export
behavior still need focused execution tests and a review of the catalog descriptions.

## Upstream snapshot

The test project's `Upstream/Substrait/` directory contains the required upstream
schemas, function definitions and type grammar, with the Apache 2.0 license.
`upstream.json` records the exact Git revision and file list. Validation uses only
these checked-in files and runs offline after NuGet restore. It needs neither Python
nor Java.

When updating Substrait, update the snapshot and manifest together. Add newly referenced
standard extension files to the snapshot and manifest. References to standard URNs use
the pinned definitions, including the two URNs with historical copies in `extensions/`.

The C# files in the test project's `Grammar/` directory were generated from that
snapshot with ANTLR 4.13.1. Regenerate them when changing the grammar. This maintenance
step requires Java and the ANTLR tool JAR, but ordinary validation does not:

```sh
cd tests/FlowtideDotNet.Substrait.Dialect.Tests/Upstream/Substrait/grammar
java -jar /path/to/antlr-4.13.1-complete.jar -Dlanguage=CSharp -package FlowtideDotNet.Substrait.Dialect.Tests.Grammar -no-listener -Xexact-output-dir -o ../../../Grammar SubstraitType.g4
```

Keep `SubstraitTypeLexer.cs` and `SubstraitTypeParser.cs`; the generated `.interp` and
`.tokens` files are not used by the .NET tests. The ANTLR runtime version is managed
centrally in `Directory.Packages.props`.
