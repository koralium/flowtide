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

using System.Text;
using FlowtideDotNet.Substrait.Documentation.Catalog;
using YamlDotNet.Core;

namespace FlowtideDotNet.Substrait.Documentation;

internal static class Program
{
    public static int Main(string[] args)
    {
        try
        {
            string? root = null;
            var check = false;
            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--root" when i + 1 < args.Length: root = Path.GetFullPath(args[++i]); break;
                    case "--check": check = true; break;
                    case "--help":
                        Console.WriteLine("Generate SQL documentation from substrait/dialect.yaml.\nOptions: --root <repository>  --check (verify without writing)");
                        return 0;
                    default: throw new ArgumentException($"Unknown or incomplete option '{args[i]}'. Use --help.");
                }
            }
            root ??= FindRoot();
            var catalog = DialectCatalog.Load(root, Path.Combine(AppContext.BaseDirectory, "Upstream", "Substrait"));
            var issues = catalog.ValidateSchema(catalog.Dialect, dialect: true)
                .Concat(catalog.LocalExtensions.Concat(catalog.UpstreamExtensions).SelectMany(doc => catalog.ValidateSchema(doc, dialect: false)))
                .Concat(catalog.ValidateTableMetadata()).Concat(catalog.ValidateReferences()).Concat(catalog.ValidateTypes()).ToArray();
            if (issues.Length != 0)
                throw new InvalidDataException(string.Join(Environment.NewLine, issues.Select(issue => issue.ToString())));
            var introduction = File.ReadAllText(Path.Combine(root, "docs", "reference", "expressions.md"));
            return WriteFiles(root, ReferenceGenerator.Generate(catalog, introduction), check, Console.Out);
        }
        catch (ArgumentException error)
        {
            return ReportError(error);
        }
        catch (InvalidDataException error)
        {
            return ReportError(error);
        }
        catch (IOException error)
        {
            return ReportError(error);
        }
        catch (UnauthorizedAccessException error)
        {
            return ReportError(error);
        }
        catch (YamlException error)
        {
            return ReportError(error);
        }
    }

    private static int ReportError(Exception error)
    {
        Console.Error.WriteLine(error.Message);
        return 1;
    }

    private static string FindRoot()
    {
        for (var directory = new DirectoryInfo(Directory.GetCurrentDirectory()); directory != null; directory = directory.Parent)
            if (File.Exists(Path.Combine(directory.FullName, "substrait", "dialect.yaml"))) return directory.FullName;
        throw new DirectoryNotFoundException("Cannot find substrait/dialect.yaml. Run from the repository or supply --root.");
    }

    internal static int WriteFiles(string root, IReadOnlyDictionary<string, string> files, bool check, TextWriter output)
    {
        var differences = 0;
        foreach (var (relativePath, content) in files)
        {
            var path = Path.Combine(root, relativePath);
            if (File.Exists(path) && Normalize(File.ReadAllText(path)) == Normalize(content)) continue;
            differences++;
            if (check) output.WriteLine($"Stale or missing generated file: {relativePath}");
            else
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, Normalize(content), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
                output.WriteLine($"Generated {relativePath}");
            }
        }
        if (check && differences > 0)
            output.WriteLine("Regenerate with: dotnet run --project tools/FlowtideDotNet.Substrait.DocGenerator --framework net10.0");
        else output.WriteLine("SQL reference files are current.");
        return check && differences > 0 ? 1 : 0;
    }

    private static string Normalize(string text) => text.Replace("\r\n", "\n");
}
