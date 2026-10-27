# Flowtide documentation

This site is built with [DocFX](https://dotnet.github.io/docfx/).

## Structure

* `index.md` / `toc.yml` — landing page and top navigation.
* `docs/` — conceptual documentation (with `docs/toc.yml` defining the sidebar).
* `releasenotes/` — release notes.
* `api/` — .NET API reference, generated from the source XML doc comments (`api/*.yml` is generated and git-ignored).
* `images/` — shared image/logo assets.
* `llms.txt` — entry point for AI assistants, published at `/flowtide/llms.txt`.
* `llms/*.txt` — focused UTF-8 Markdown references for streams, SQL, expressions, and connectors, copied unchanged into the site as DocFX resources.
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
# Build the site (generates API metadata + static site into _site/)
docfx docfx.json

# Build and serve at http://localhost:8080
docfx docfx.json --serve
```

To regenerate only the API metadata:

```bash
docfx metadata docfx.json
```

## Deployment

Pushing to `main` triggers `.github/workflows/docs.yml`, which builds the site
and publishes `docs/_site` to the `gh-pages` branch.

## Maintaining the LLM references

Keep `llms.txt` as a short navigation file; put detailed examples and constraints in
the relevant `llms/*.txt` guide. These are maintained source files, not generated
API documentation. Update them when public builder APIs, SQL syntax, function
registrations, connector contracts, or supported target frameworks change.
Check behavior against `src/FlowtideDotNet.Substrait/Sql`, the runtime functions,
`tests/FlowtideDotNet.AcceptanceTests`, and the relevant samples/connector tests.
Distinguish current repository features from availability in released packages.

Use absolute documentation URLs including the `/flowtide/` project prefix.
Link to `.md` for Markdown-authored pages and `.txt` for the LLM guides.
After `docfx build docfx.json`, verify
that `_site/llms.txt` and all four `_site/llms/*.txt` files match their source
files, and that local documentation links resolve in `_site`. No separate
deployment step is needed for these resources.

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
