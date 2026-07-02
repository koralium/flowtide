# Flowtide documentation

This site is built with [DocFX](https://dotnet.github.io/docfx/).

## Structure

* `index.md` / `toc.yml` — landing page and top navigation.
* `docs/` — conceptual documentation (with `docs/toc.yml` defining the sidebar).
* `releasenotes/` — release notes.
* `api/` — .NET API reference, generated from the source XML doc comments (`api/*.yml` is generated and git-ignored).
* `images/` — shared image/logo assets.
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

# Build and serve with live preview at http://localhost:8080
docfx docfx.json --serve
```

To regenerate only the API metadata:

```bash
docfx metadata docfx.json
```

## Deployment

Pushing to `main` triggers `.github/workflows/docs.yml`, which builds the site
and publishes `docs/_site` to the `gh-pages` branch.
