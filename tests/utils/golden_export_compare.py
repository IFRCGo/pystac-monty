"""Compare on-disk batch export trees to golden JSON under ``monty-stac-extension/examples``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast


def _basename_href(href: str) -> str:
    if not href:
        return href
    path = href.split("?")[0]
    base = path.rstrip("/").rsplit("/", 1)[-1]
    if base.endswith(".json"):
        return base
    return f"{base}.json"


def _normalize_href(href: str) -> str:
    if href.startswith(("http://", "https://")) and not href.rstrip("/").split("?")[0].endswith(".json"):
        return href
    if href.endswith(".json") or "/../" in href or href.startswith(("./", "../")):
        return _basename_href(href)
    return href


def _round_floats(x: Any, ndigits: int = 10) -> Any:
    if isinstance(x, float):
        return round(x, ndigits)
    if isinstance(x, list):
        return [_round_floats(i, ndigits) for i in x]
    if isinstance(x, dict):
        return {k: _round_floats(v, ndigits) for k, v in x.items()}
    return x


def _sort_if_all_strings(xs: list[Any]) -> list[Any]:
    if xs and all(isinstance(i, str) for i in xs):
        return sorted(xs)
    return xs


def _normalize_links(links: list[Any]) -> list[Any]:
    out = []
    for link in links:
        if not isinstance(link, dict):
            out.append(link)
            continue
        rel = str(link.get("rel", ""))
        if rel in ("root", "collection"):
            continue
        d = {k: v for k, v in link.items() if k not in ("type", "title")}
        if "href" in d and isinstance(d["href"], str):
            d["href"] = _normalize_href(d["href"])
        out.append(d)
    out.sort(
        key=lambda d: (
            d.get("rel", "") if isinstance(d, dict) else "",
            d.get("href", "") if isinstance(d, dict) else "",
            json.dumps(d.get("roles"), sort_keys=True) if isinstance(d, dict) else "",
        )
    )
    return out


def _normalize_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        d: dict[str, Any] = {}
        for k, v in obj.items():
            if k == "stac_version":
                continue
            if k == "links" and isinstance(v, list):
                d[k] = _normalize_links(v)
            else:
                d[k] = _normalize_obj(v)
        for key in ("keywords", "monty:country_codes", "monty:hazard_codes"):
            if key in d and isinstance(d[key], list):
                d[key] = _sort_if_all_strings(d[key])
        if "summaries" in d and isinstance(d["summaries"], dict):
            s = dict(d["summaries"])
            for key in ("keywords", "monty:country_codes", "monty:hazard_codes", "roles"):
                if key in s and isinstance(s[key], list):
                    s[key] = _sort_if_all_strings(s[key])
            d["summaries"] = s
        return dict(sorted(d.items()))
    if isinstance(obj, list):
        return [_normalize_obj(i) for i in obj]
    return obj


def normalize_stac_json(doc: dict[str, Any]) -> dict[str, Any]:
    """Return a canonical form suitable for equality with another export of the same pipeline."""
    return cast(dict[str, Any], _round_floats(_normalize_obj(doc)))


def load_json_tree(subdir: Path) -> dict[str, dict[str, Any]]:
    """``filename`` -> parsed JSON for each ``*.json`` directly under *subdir*."""
    if not subdir.is_dir():
        return {}
    return {p.name: json.loads(p.read_text(encoding="utf-8")) for p in sorted(subdir.glob("*.json"))}


def assert_golden_export_subtrees_match(
    *,
    examples_dir: Path,
    produced_dir: Path,
    subdirs: tuple[str, ...],
) -> None:
    """Require the same set of JSON filenames per subdir and matching normalized documents."""
    import pytest

    for sub in subdirs:
        gold_path = examples_dir / sub
        prod_path = produced_dir / sub
        if not gold_path.is_dir():
            pytest.skip(f"Golden examples missing: {gold_path}")
        if not prod_path.is_dir():
            pytest.fail(f"Expected produced subdirectory {prod_path!s}")

        golden = load_json_tree(gold_path)
        produced = load_json_tree(prod_path)
        g_keys, p_keys = set(golden), set(produced)
        if g_keys != p_keys:
            pytest.fail(
                f"{sub}: golden vs produced filename mismatch.\n"
                f"  only in golden: {sorted(g_keys - p_keys)}\n"
                f"  only in produced: {sorted(p_keys - g_keys)}"
            )
        for name in sorted(golden):
            g_norm = normalize_stac_json(golden[name])
            p_norm = normalize_stac_json(produced[name])
            if g_norm != p_norm:
                pytest.fail(
                    f"{sub}/{name}: normalized JSON differs from golden examples.\n"
                    "Re-run export into monty-stac-extension/examples or update golden files."
                )
