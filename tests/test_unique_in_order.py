"""Order of values written into items must not depend on set iteration order."""

import os
import subprocess
import sys

from pystac_monty.sources.utils import unique_in_order


def test_keeps_first_occurrence_order() -> None:
    assert unique_in_order(["JAM", "CUB", " JAM ", "BMU", "", None, "CUB", "HTI"]) == ["JAM", "CUB", "BMU", "HTI"]


def test_keeps_non_string_values() -> None:
    assert unique_in_order(["a", 1, 1, "a", 2.5]) == ["a", 1, 2.5]


def test_same_result_under_different_hash_seeds() -> None:
    code = "from pystac_monty.sources.utils import unique_in_order; print(unique_in_order(['JAM','CUB','BMU','HTI','CAN']))"
    outputs = set()
    for seed in ("1", "2", "3", "4"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        outputs.add(subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True).stdout)
    assert outputs == {"['JAM', 'CUB', 'BMU', 'HTI', 'CAN']\n"}
