# Contributing to pystac-monty

pystac-monty is one half of a system. The other half is [monty-stac-extension](https://github.com/IFRCGo/monty-stac-extension): the specification, the taxonomy, one analysis document for each source, and the worked `examples/`. That repository is a git submodule here (`monty-stac-extension/`).

A transformer is the code form of its source analysis. When a transformer changes what it writes, the source doc and the examples must change with it. This file gives the steps.

## Set up

```sh
git submodule update --init
uv sync --all-extras
uv run pytest tests
```

Some tests replay recorded HTTP calls (cassettes). To record them again, run `uv run pytest --record-mode rewrite`.

## Commits and releases

- The PR title must follow [Conventional Commits](https://www.conventionalcommits.org/), for example `fix(gdacs): …`. A CI job checks it, and the PR is squash-merged with that title.
- [release-please](https://github.com/googleapis/release-please) makes the releases and writes `CHANGELOG.md` from the PR titles. Do not edit `CHANGELOG.md`.

## When a transformer changes its output

An output change is a change to an item id, a field value, a hazard, impact or response code, a link, or the number of items for one source record. Refactors and fixes that do not change the items do not need these steps.

1. **Open the PR here**, with the transformer change and its tests.
2. **Open a PR in monty-stac-extension** with:
   - the source doc: `docs/model/sources/<Source>/README.md`;
   - the examples, regenerated from your branch (see below);
   - `taxonomy.md` or `response-taxonomy.md` first, if a code changed. The `hazard-profiles-taxonomy-drift` job here compares `HazardProfiles.csv` with `taxonomy.md`;
   - a `CHANGELOG.md` entry, written by hand in that repository.
3. **Point the submodule here at the head commit of that PR.** The CI here then tests your transformer against the new examples.
4. **Merge the monty-stac-extension PR first.** Then point the submodule at its merge commit on `main`, and merge this PR.

The order in step 4 is necessary. monty-stac-extension uses squash merges, so the commits of a PR branch are not on `main` after the merge. A submodule that points at a branch commit breaks when the branch is deleted.

### Regenerate the examples

`tests/test_extension_examples.py` runs the checker of monty-stac-extension (`scripts/check_example_drift.py`) for each source that has a `regenerate:` recipe in its `.github/etl-watch.yml`. The `extension-examples` job runs it in CI. If it fails because your change is intended, regenerate the examples in a monty-stac-extension checkout, with your branch of pystac-monty installed:

```sh
cd ../monty-stac-extension
pip install -e ../pystac-monty
python scripts/check_example_drift.py --source gdacs --diff    # look at the changes
python scripts/check_example_drift.py --source gdacs --write   # write them to examples/
npm test
```

`--write` changes only the example files that exist. If an item id changed, rename the example by hand.

For a source without a recipe, change the examples by hand, or add a recipe: it needs a batch exporter in `pystac_monty/sources/batch_export.py` and a fixture in monty-stac-extension that is valid JSON.

### What monty-stac-extension does on its side

- Each day, `scripts/check_etl_drift.py` compares the commits here with the commit that each source doc was last reviewed against. It opens a "Source doc drift" issue for each source whose transformer changed.
- `check_etl_drift.py --pull N` checks one PR here for changes to item ids or mappings, before the merge.

Each of these finds a change after you made it. The steps above make sure that the doc and the examples change in the same PR as the code.

## The transformer and its source doc

Each transformer names its source analysis, and the source analysis names its transformer:

- In a transformer class, `source_name` is the `id` of the source in monty-stac-extension (`docs/model/sources/sources.yml`), and `source_doc` is the path of its source doc in that repository.
- In `sources.yml`, `doc` is the source doc and `etl` is the URL of the transformer file here.

`tests/test_source_docs.py` checks that the two sides agree. It fails for a transformer with no source analysis, and for an entry of `sources.yml` whose `etl` names a file that does not exist.

A new source starts with a source analysis in monty-stac-extension (use its "New source" issue form). Add the transformer here after that, with `source_name` and `source_doc`, and set `etl` in `sources.yml`.

## Report a mapping decision

If you find something about a source while you work on its transformer (an undocumented value, a code that is wrong, a field that is always empty), write it in the source doc in monty-stac-extension, not only in a commit message or a code comment. The source doc is the place where the next person looks.
