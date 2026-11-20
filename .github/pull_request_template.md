## What and why

<!-- What changes, and why. Link the issue: "Closes #123". -->

## monty-stac-extension

Does this PR change what a transformer writes? For example: an item id, a field value, a hazard, impact or response code, a link, or the number of items for one source record.

- [ ] No. The rest of this section does not apply.
- [ ] Yes. Then:
  - [ ] The PR in monty-stac-extension is: IFRCGo/monty-stac-extension#
  - [ ] It updates the source doc (`docs/model/sources/<Source>/README.md`) and regenerates the affected `examples/`.
  - [ ] If a hazard, impact or response code changed, it also updates `taxonomy.md` or `response-taxonomy.md`.
  - [ ] The `monty-stac-extension` submodule here points at that PR, and later at its merge commit on `main` (see [CONTRIBUTING.md](https://github.com/IFRCGo/pystac-monty/blob/main/CONTRIBUTING.md)).

## Tests

- [ ] `uv run pytest tests` passes.
