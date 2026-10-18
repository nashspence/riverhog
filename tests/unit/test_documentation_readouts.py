"""Native observations stay tied to the selected transform, products and parity evidence."""

import argparse
import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

from tests.documentation_fixtures import closure, document

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from contract_atlas import documentation_artifacts as artifacts  # noqa: E402
from contract_atlas.documentation_markdown import compile_corpus  # noqa: E402
from contract_atlas.documentation_native import (  # noqa: E402
    apply_cli_prose,
    expected_outputs,
    extract_cli,
    native_semantics,
    native_slots,
)
from contract_atlas.documentation_requirements import build_requirements  # noqa: E402
from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402


@pytest.fixture
def retained_readouts(tmp_path, monkeypatch):
    import contract_freeze

    code = closure()
    code["external_contract"]["release"]["publication"] = {"runtime_images": {}}
    requirements = build_requirements(code)
    compiled = compile_corpus(
        {
            "reference.md": document(
                [
                    {"target": row["target"], "summary": "Literal 50% %(prog)s [text]."}
                    for row in requirements["subjects"].values()
                    if row["rule"] == "authored"
                ]
            )
        },
        requirements,
    )
    slots = native_slots(compiled, requirements, "v1.0.0")
    parser = argparse.ArgumentParser(prog="fixture")
    parser.add_argument("--archive", default="source")
    parser.add_argument("--secret", help=argparse.SUPPRESS)
    shape = contract_freeze._argparse_command(parser, name="fixture")
    apply_cli_prose({"fixture": parser}, compiled, requirements)
    observed, help_text = extract_cli({"fixture": parser})
    expected = expected_outputs(slots)
    observed = {name: observed[name] for name in expected}
    semantics = native_semantics({"cli": {"fixture": shape}, "python": {}, "openapi": {}})
    request = {"modules": ["fixture"], "python": {}, "openapi": {}, "metadata": []}
    monkeypatch.setattr(artifacts, "native_request", lambda _code, _plan: request)
    actual = {
        "observed": observed,
        "readouts": {"cli": help_text, "python": {}, "openapi": {}, "metadata": {}},
        "semantics": semantics,
        "installed_modules": ["fixture"],
        "cli_journeys": {
            "fixture": {"argv": ["fixture", "--help"], "exit": 0, "stdout": help_text["fixture"]}
        },
    }
    capture = {"commit": "b" * 40, "tag": "v1.0.0", "files": compiled["source_files"]}
    parity = artifacts.verify_readout_semantics(semantics, semantics, request)
    files = {
        "documentation-record.json": {"compiled": compiled, "requirements": requirements},
        "documentation-preparation-plan.json": {"slots": slots, "source_capture": capture},
        "native-source-semantics.json": semantics,
        "native-readouts.json": actual,
        "native-oci.json": {},
        "native-semantic-parity.json": parity,
    }
    build = {
        "source_sha": "a" * 40,
        "documentation": {
            "tag": "v1.0.0",
            "commit": "b" * 40,
            "source_files": compiled["source_files"],
        },
        "files": {},
    }
    for name, value in files.items():
        raw = canonical_bytes(value)
        (tmp_path / name).write_bytes(raw)
        build["files"][name] = hashlib.sha256(raw).hexdigest()
    record = {
        "current": {
            "expected": expected,
            "observed": observed,
            "identity": {"compiler": {"fixture": "native argparse reader"}},
            "artifacts": {
                "source_sha": build["source_sha"],
                "compiler": {"fixture": "native argparse reader"},
                "readouts": {
                    name: build["files"][name]
                    for name in (
                        "native-readouts.json",
                        "native-oci.json",
                        "native-semantic-parity.json",
                    )
                },
                "semantic_parity": parity,
                "products": {"fixture.whl": "d" * 64},
            },
        }
    }
    return tmp_path, code, record, build


def test_readout_verification_reconciles_actual_native_values(retained_readouts):
    artifacts.verify_prepared_readouts(*retained_readouts)


@pytest.mark.parametrize(
    "mutation",
    [
        "code",
        "corpus",
        "compiler",
        "missing-readout",
        "readout-bytes",
        "transform",
        "observation",
        "semantics",
        "failed-journey",
        "missing-module",
    ],
)
def test_native_evidence_cannot_be_reused_for_a_different_tuple(retained_readouts, mutation):
    root, code, record, build = retained_readouts
    if mutation == "code":
        build["source_sha"] = "f" * 40
    elif mutation == "corpus":
        build["documentation"]["commit"] = "f" * 40
    elif mutation == "compiler":
        record["current"]["identity"]["compiler"] = {"different": "compiler"}
    elif mutation == "missing-readout":
        record["current"]["artifacts"]["readouts"].pop("native-oci.json")
    elif mutation == "readout-bytes":
        build["files"]["native-readouts.json"] = "f" * 64
    elif mutation == "observation":
        record["current"]["observed"] = copy.deepcopy(record["current"]["observed"])
        record["current"]["observed"][next(iter(record["current"]["observed"]))] = "Different text."
    else:
        name = (
            "documentation-preparation-plan.json"
            if mutation == "transform"
            else "native-readouts.json"
        )
        data = json.loads((root / name).read_bytes())
        if mutation == "transform":
            data["slots"]["cli"]["fixture"]["summary"] = "Different text."
        elif mutation == "semantics":
            data["semantics"]["cli"]["value"]["fixture"]["name"] = "Different command"
        elif mutation == "failed-journey":
            data["cli_journeys"]["fixture"]["exit"] = 2
        elif mutation == "missing-module":
            data["installed_modules"] = []
        raw = canonical_bytes(data)
        (root / name).write_bytes(raw)
        build["files"][name] = hashlib.sha256(raw).hexdigest()
        if name in record["current"]["artifacts"]["readouts"]:
            record["current"]["artifacts"]["readouts"][name] = build["files"][name]
    with pytest.raises(ContractAtlasError):
        artifacts.verify_prepared_readouts(root, code, record, build)
