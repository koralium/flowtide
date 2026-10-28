# Flowtide documentation

This site is built with [DocFX](https://dotnet.github.io/docfx/).

## Structure

* `index.md` / `toc.yml` — landing page and top navigation.
* `docs/` — conceptual documentation (with `docs/toc.yml` defining the sidebar).
* `releasenotes/` — release notes.
* `api/` — .NET API reference, generated from the source XML doc comments (`api/*.yml` is generated and git-ignored).
* `images/` — shared image/logo assets.
* `llms.txt` — entry point for AI assistants, published at `/flowtide/llms.txt`.
* `llms/*.txt` — focused UTF-8 Markdown references for streams, SQL, expressions, and connectors, copied unchanged into the site as DocFX resources. The expressions guide is generated from the Substrait catalog and `reference/expressions.md`.
* `docs/expressions/functions-reference.md` — generated SQL function reference, with the same content as `llms/expressions.txt`.
* `reference/` — handwritten expressions introduction and generated XML includes for NuGet documentation.
* `llms-full.yml` — ordered list of the four guides; DocFX renders it directly to `_site/llms-full.txt`, published at `/flowtide/llms-full.txt`.
* `templates/flowtide/` — site styling and build-time templates for static LLM discovery links.
* `docfx.json` — build configuration; `filterConfig.yml` — API reference filter.

## Prerequisites

DocFX is a .NET global tool. The .NET 8 and .NET 10 SDKs are required to generate the API reference.

```bash
dotnet tool update -g docfx
```

## Build and serve locally

From the `docs/` directory:

```bash
# Generate API metadata, llms-full.txt, and the static site into _site/
docfx docfx.json

# Build and serve at http://localhost:8080
docfx docfx.json --serve

# Rebuild the site using existing API metadata
docfx build docfx.json
```

To regenerate only the API metadata:

```bash
docfx metadata docfx.json
```

## Deployment

Pushing to `main` triggers `.github/workflows/docs.yml`, which builds the site
and publishes `docs/_site` to the `gh-pages` branch.

## Maintaining the LLM references

Keep `llms.txt` as a short navigation file. The streams, SQL and connectors guides
are handwritten. The expressions guide combines `reference/expressions.md` with
the function profiles in `../substrait/dialect.yaml` and their extension definitions.
Edit the catalog for function signatures, options, examples and restrictions;
edit `reference/expressions.md` for general expression guidance.
Update the handwritten guides when public builder APIs, SQL syntax,
connector contracts or supported target frameworks change.
Check behavior against `src/FlowtideDotNet.Substrait/Sql`, the runtime functions,
`tests/FlowtideDotNet.AcceptanceTests`, and the relevant samples/connector tests.
Distinguish current repository features from availability in released packages.

After changing the catalog or expressions introduction, run from the repository root:

```sh
dotnet run --project tools/FlowtideDotNet.Substrait.DocGenerator --framework net10.0
# Validate the catalog and verify generated files without modifying them:
dotnet run --project tools/FlowtideDotNet.Substrait.DocGenerator --framework net10.0 -- --check
```

Commit the generated Markdown, text and XML outputs together with the source change.
CI and the documentation deployment check for stale output. Checked-in outputs keep
`docfx docfx.json`, `docfx build docfx.json` and ordinary NuGet builds usable directly.
The generator supports .NET 8 and .NET 10 and needs no Python or Java.

`reference/sql-functions.xml` is compiled into the XML documentation of
`FlowtideDotNet.Substrait` and `FlowtideDotNet.DependencyInjection`. Those XML files
ship beside the assemblies in the NuGet packages; consumers need neither this
repository nor the original include file. `SqlFunctionReference` and its nested
`Scalar`, `Aggregate`, `Window` and `Table` reference types expose the full catalog.
`SqlPlanBuilder.Sql`, `FunctionRegister` and `AddSqlTextAsPlan` include the signature
index and link to those types. `AddFlowtideStream` explains the required plan,
connectors and state storage, with a complete host example and a link to the SQL reference.
Editor/assistant use of XML
documentation depends on the client; package verification checks the content itself.

Use absolute documentation URLs including the `/flowtide/` project prefix.
Link to `.md` for Markdown-authored pages and `.txt` for the LLM guides.
After `docfx build docfx.json`, verify that `_site/llms.txt` and all four
`_site/llms/*.txt` files match their sources, that `_site/llms-full.txt` contains
the complete guides in order, and that local documentation links resolve. No separate
deployment step is needed for these resources.

DocFX generates `llms-full.txt` in a fixed order: streams, SQL, expressions, then
connectors. `llms-full.yml` declares the input files. The custom document schema
`templates/flowtide/schemas/FlowtideLlmBundle.schema.json` uses `reference: file`
to read each guide's contents without converting Markdown to HTML.
`FlowtideLlmBundle.txt.primary.tmpl` combines those contents with source links
and section separators. Its triple-brace content field preserves Markdown,
code examples, and special characters without HTML escaping.

The bundle is produced directly in the site output during the normal DocFX
build, including `docfx docfx.json --serve`. A missing input guide fails the
build. Regenerate the expressions guide when its catalog inputs change, then
rebuild; do not edit or commit the generated bundle. The bundle itself needs no
wrapper project, compiled plugin, Python or Node.js dependency.

DocFX also copies the original Markdown pages as resources in the `markdown`
group, with that group's destination set to `.`. The separate group lets a
source page be rendered as HTML and copied as Markdown in one build without
duplicate-file warnings. The resulting `page.md` sits alongside `page.html`;
original front matter, code examples, and relative Markdown links are preserved.

`templates/flowtide/layout/_master.tmpl` adds `rel="describedby"` links to
`llms.txt`. `conceptual.extension.js` runs inside DocFX and supplies each
Markdown-authored page's `rel="alternate" type="text/markdown"` target.
Generated API pages have no Markdown alternate unless they originate from
Markdown. The links are present in static HTML; no browser JavaScript, Node.js,
Python, or post-build script is required. The layout is based on DocFX 2.78.5's
modern template; compare it with the upstream layout when upgrading DocFX.

The repository-root `../llms.txt` is an optional entry point for agents browsing
the source checkout. The canonical site index remains `docs/llms.txt`, which
DocFX publishes at the website's `/flowtide/llms.txt` path.
