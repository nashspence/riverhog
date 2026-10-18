"""Isolated native extraction, semantic parity, and a readable prepared review packet."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from .documentation_native import (
    native_semantics,
    public_object,
    select_observations,
    verify_semantic_parity,
)
from .generation import ROOT, qualify_documentation_candidate, verify_candidate
from .model import (
    ContractAtlasError,
    _decode_unsafe_integers,
    canonical_bytes,
    canonical_sha256,
    pointer_value,
)

# Versions come from the coordinator's exported lock constraints; these tools
# read native surfaces and release declarations inside the isolated interpreter.
READER_DEPENDENCIES = ("markdown-it-py", "PyYAML", "httpx", "packaging", "license-expression")


def native_request(closure: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
    import contract_freeze

    python = {}
    for element in closure["elements"]:
        if element["interface"] == "python" and element["title"] in plan["slots"]["python"]:
            slot = plan["slots"]["python"][element["title"]]
            python[element["title"]] = {
                "surface": pointer_value(closure, element["pointers"][0]),
                "capability": slot["capability"],
            }
    modules = {
        *contract_freeze.CLI_MODULES.values(),
        *(v["surface"]["module"] for v in python.values()),
    }
    return {
        "modules": sorted(modules),
        "python": python,
        "openapi": {name: sorted(fields) for name, fields in plan["slots"]["openapi"].items()},
        "metadata": sorted(plan["slots"]["metadata"]),
    }


def source_native_semantics(
    closure: Mapping[str, Any], request: Mapping[str, Any]
) -> dict[str, Any]:
    import contract_freeze

    parsers = contract_freeze._cli_parsers()
    decoded = cast(
        dict[str, Any], _decode_unsafe_integers(closure, closure["unsafe_integer_paths"])
    )
    return native_semantics(
        {
            "cli": {
                name: contract_freeze._argparse_command(parser, name=name)
                if isinstance(parser, argparse.ArgumentParser)
                else contract_freeze._click_command(parser, name=name)
                for name, parser in parsers.items()
            },
            "openapi": decoded["external_contract"]["http_openapi"],
            "python": {
                identity: contract_freeze._python_export(value)
                for identity, row in request["python"].items()
                for value in [public_object(row["surface"])]
            },
        }
    )


def _openapi_semantics(value: Mapping[str, Any], pointers: list[str]) -> dict[str, Any]:
    result = copy.deepcopy(dict(value["value"]))
    for pointer in pointers:
        node: Any = result
        for part in pointer.split("/")[1:]:
            key = part.replace("~1", "/").replace("~0", "~")
            node = node[int(key)] if isinstance(node, list) else node[key]
        # Remove only typed recognized annotations, never wire property names or extensions.
        node.pop("description", None)
        if pointer.startswith("/paths/") and pointer.count("/") == 3:
            node.pop("summary", None)
    return {"value": result, "unsafe_integer_paths": value["unsafe_integer_paths"]}


def verify_readout_semantics(
    source: Mapping[str, Any], actual: Mapping[str, Any], request: Mapping[str, Any]
) -> dict[str, Any]:
    proof = {}
    for kind in ("cli", "python"):
        verify_semantic_parity(source[kind], actual[kind], destination=kind)
        proof[kind] = {
            "source_sha256": canonical_sha256(source[kind]),
            "installed_sha256": canonical_sha256(actual[kind]),
        }
    for name, pointers in request["openapi"].items():
        before = _openapi_semantics(source["openapi"][name], pointers)
        after = _openapi_semantics(actual["openapi"][name], pointers)
        verify_semantic_parity(before, after, destination="served OpenAPI " + name)
        proof["openapi/" + name] = {
            "source_sha256": canonical_sha256(before),
            "installed_sha256": canonical_sha256(after),
        }
    return proof


def verify_prepared_readouts(
    candidate: Path,
    closure: Mapping[str, Any],
    record: Mapping[str, Any],
    build: Mapping[str, Any],
) -> None:
    """Reconcile retained observations with their exact artifact/readout/transform tuple."""
    from .documentation_native import native_slots

    snapshot = record["current"]
    artifacts = snapshot["artifacts"]
    required = {"native-readouts.json", "native-oci.json", "native-semantic-parity.json"}
    if set(artifacts.get("readouts", {})) != required or not required | {
        "documentation-preparation-plan.json",
        "native-source-semantics.json",
    } <= set(build["files"]):
        raise ContractAtlasError("prepared documentation lacks complete native readout evidence")
    for name, digest in artifacts["readouts"].items():
        if build["files"].get(name) != digest:
            raise ContractAtlasError("prepared native readout differs from its audit: " + name)
    if artifacts.get("source_sha") != build["source_sha"] or artifacts.get("compiler") != snapshot[
        "identity"
    ].get("compiler"):
        raise ContractAtlasError("prepared native evidence belongs to different inputs")
    actual = json.loads((candidate / "native-readouts.json").read_bytes())
    oci = json.loads((candidate / "native-oci.json").read_bytes())
    plan = json.loads((candidate / "documentation-preparation-plan.json").read_bytes())
    document = json.loads((candidate / "documentation-record.json").read_bytes())
    if plan["slots"] != native_slots(
        document["compiled"], document["requirements"], build["documentation"]["tag"]
    ) or plan.get("source_capture") != {
        "commit": build["documentation"]["commit"],
        "tag": build["documentation"]["tag"],
        "files": build["documentation"]["source_files"],
    }:
        raise ContractAtlasError("prepared native transform differs from captured documentation")
    request = native_request(closure, plan)
    if actual.get("installed_modules") != sorted(request["modules"]):
        raise ContractAtlasError("prepared native extraction omitted installed module ownership")
    source = json.loads((candidate / "native-source-semantics.json").read_bytes())
    parity = verify_readout_semantics(source, actual["semantics"], request)
    retained_parity = json.loads((candidate / "native-semantic-parity.json").read_bytes())
    if parity != retained_parity or parity != artifacts.get("semantic_parity"):
        raise ContractAtlasError("prepared semantic parity differs from native evidence")
    observed = dict(actual["observed"])
    for name, value in actual["readouts"]["python"].items():
        if observed.get("python/" + name) != value:
            raise ContractAtlasError("prepared Python readout differs from its observation")
    from email.parser import Parser

    for name, raw in actual["readouts"]["metadata"].items():
        metadata = Parser().parsestr(raw)
        if observed.get("metadata/" + name) != {
            "summary": metadata["Summary"],
            "description": cast(str, metadata.get_payload()).rstrip("\n"),
        }:
            raise ContractAtlasError("prepared metadata readout differs from its observation")
    for authority, pointers in request["openapi"].items():
        served = actual["readouts"]["openapi"][authority]
        if served != actual["semantics"]["openapi"][authority]:
            raise ContractAtlasError("prepared served OpenAPI evidence differs")
        document = served["value"]
        for pointer in pointers:
            node = cast(Mapping[str, Any], pointer_value(document, pointer))
            if observed.get("openapi/" + authority + pointer) != node.get("description"):
                raise ContractAtlasError("prepared OpenAPI annotation differs from its observation")
    cli_names = set(source["cli"]["value"])
    if set(actual.get("cli_journeys", {})) != cli_names or any(
        journey.get("argv") != [name, "--help"]
        or journey.get("exit") != 0
        or not journey.get("stdout", "").strip()
        for name, journey in actual["cli_journeys"].items()
    ):
        raise ContractAtlasError("prepared CLI journeys are missing or failed")
    image_names = set(closure["external_contract"]["release"]["publication"]["runtime_images"])
    if set(oci) != image_names:
        raise ContractAtlasError("prepared OCI observations omit declared runtime images")
    for name, image in oci.items():
        if artifacts["products"].get("oci/" + name) != image["image_id"]:
            raise ContractAtlasError("prepared OCI observation belongs to a different ImageID")
        observed["oci/" + name] = image["labels"].get("org.opencontainers.image.description")
    if select_observations(snapshot["expected"], observed) != snapshot["observed"]:
        raise ContractAtlasError("prepared observations differ from retained native readouts")


def inspect_prepared_artifacts(
    candidate: Path,
    distributions: Path,
    plan: Mapping[str, Any],
    source_semantics: Mapping[str, Any],
    *,
    image_records: list[dict[str, Any]],
    initial: bool,
    baseline: Any = None,
    custody: Any = None,
) -> dict[str, Any]:
    build = cast(dict[str, Any], verify_candidate(candidate))
    closure = json.loads((candidate / "riverhog-v1.json").read_bytes())
    request = native_request(closure, plan)
    wheels = sorted(distributions.glob("*.whl"))
    if not wheels:
        raise ContractAtlasError("prepared documentation requires actual distribution wheels")
    products = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(distributions.iterdir())
        if p.is_file()
    }
    with tempfile.TemporaryDirectory(prefix="riverhog-documentation-installed-") as temporary:
        scratch = Path(temporary)
        environment = scratch / "environment"
        subprocess.run(["uv", "venv", "--python", sys.executable, str(environment)], check=True)
        interpreter = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        # Tooling dependencies are locked coordinator readers, not component dependencies.
        constraints = scratch / "locked-dependencies.txt"
        subprocess.run(
            [
                "uv",
                "export",
                "--locked",
                "--all-packages",
                "--all-groups",
                "--no-emit-workspace",
                "--no-hashes",
                "--output-file",
                str(constraints),
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(interpreter),
                "--constraint",
                str(constraints),
                *map(str, wheels),
                *READER_DEPENDENCIES,
            ],
            check=True,
        )
        (scratch / "request.json").write_bytes(canonical_bytes(request))
        subprocess.run(
            [
                str(interpreter),
                "-I",
                str(ROOT / "scripts/documentation_readouts.py"),
                "--request",
                str(scratch / "request.json"),
                "--output",
                str(scratch / "readouts.json"),
            ],
            cwd=scratch,
            env={k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "VIRTUAL_ENV"}},
            check=True,
        )
        actual = json.loads((scratch / "readouts.json").read_bytes())
    parity = verify_readout_semantics(source_semantics, actual["semantics"], request)
    observed = actual["observed"]
    oci = {}
    for record in image_records:
        digest = "sha256:" + record["sha256"]
        response = subprocess.check_output(["docker", "image", "inspect", digest], text=True)
        image = json.loads(response)[0]
        labels = image["Config"]["Labels"]
        name = next(
            name
            for name, slot in closure["external_contract"]["release"]["publication"][
                "runtime_images"
            ].items()
            if slot["repository"] == record["name"]
        )
        observed["oci/" + name] = labels.get("org.opencontainers.image.description")
        oci[name] = {"image_id": image["Id"], "labels": labels}
        products["oci/" + name] = image["Id"]
    expected = json.loads((candidate / "documentation-audit.json").read_bytes())["current"][
        "expected"
    ]
    # Expected output selects source-owned destinations only; values always come from artifacts.
    observed = select_observations(expected, observed)
    raw = {
        "native-readouts.json": canonical_bytes(actual),
        "native-oci.json": canonical_bytes(oci),
        "native-semantic-parity.json": canonical_bytes(parity),
    }
    artifacts = {
        "products": products,
        "readouts": {name: hashlib.sha256(value).hexdigest() for name, value in raw.items()},
        "semantic_parity": parity,
        "source_sha": build["source_sha"],
        "compiler": json.loads((candidate / "documentation-audit.json").read_bytes())["current"][
            "identity"
        ]["compiler"],
    }
    raw.update(review_readouts(actual["readouts"], oci, closure))
    return qualify_documentation_candidate(
        candidate,
        observations=observed,
        artifacts=artifacts,
        readouts=raw,
        initial=initial,
        baseline=baseline,
        custody=custody,
    )


def review_readouts(
    readouts: Mapping[str, Any], oci: Mapping[str, Any], closure: Mapping[str, Any]
) -> dict[str, bytes]:
    """Readable native directories and bounded destinations; full raw evidence stays intact."""
    from .html_rendering import _element_file, _esc, _link, _shell, _table_html

    pages: dict[str, bytes] = {}
    digest = canonical_sha256(closure)
    by_title = {e["title"]: e for e in closure["elements"]}

    def page(route: str, title: str, body: str) -> None:
        pages["riverhog-v1/" + route] = _shell(
            title,
            body,
            digest,
            audit=False,
            documentation=True,
            breadcrumbs='<p><a href="review.html">Prepared candidate review</a></p>',
        )

    directories = []
    for kind, values in {**readouts, "oci": oci}.items():
        route = "native-" + kind + ".html"
        directories.append("<li>" + _link(route, "Actual installed " + kind) + "</li>")
        rows = []
        for name, value in sorted(values.items()):
            element = by_title.get(name)
            canonical = (
                _link(
                    _element_file(element["id"]) + "#documentation-audit",
                    "Contract facts and selected prose",
                )
                if element
                else ""
            )
            if kind in {"cli", "metadata"}:
                destination = "native-readout-" + canonical_sha256([kind, name])[:24] + ".html"
                page(
                    destination,
                    name + " · installed " + kind,
                    canonical + "<pre>" + _esc(value) + "</pre>",
                )
                rows.append((_link(destination, name), canonical))
            elif kind == "openapi":
                value = value["value"]
                elements = [
                    e
                    for e in closure["elements"]
                    if e["interface"].startswith("http-")
                    and any("/http_openapi/" + name + "/" in p for p in e["pointers"])
                ]
                destination = "native-api-" + canonical_sha256(name)[:24] + ".html"
                page(
                    destination,
                    name + " · served OpenAPI",
                    "<p>The complete served document is in the raw native readouts. "
                    "Each canonical operation/schema shows its actual selected "
                    "projection and source-owned facts.</p>"
                    + _table_html(
                        ("Native contract", "Interface"),
                        [
                            (
                                _link(_element_file(e["id"]) + "#documentation-audit", e["title"]),
                                _esc(e["interface"]),
                            )
                            for e in elements
                        ],
                    ),
                )
                rows.append(
                    (
                        _link(destination, name),
                        _esc(
                            f"{len(value.get('paths', {}))} paths; "
                            f"{len(value.get('components', {}).get('schemas', {}))} schemas"
                        ),
                    )
                )
            elif kind == "python":
                rows.append((_esc(name), canonical + "<p>" + _esc(value["capability"]) + "</p>"))
            else:
                rows.append(
                    (
                        _esc(name),
                        "<pre>" + _esc(json.dumps(value, ensure_ascii=False, indent=2)) + "</pre>",
                    )
                )
        page(
            route,
            "Actual installed " + kind,
            '<p><a href="../native-readouts.json">Complete installed readouts</a> · '
            '<a href="../native-oci.json">Complete OCI readouts</a></p>'
            + _table_html(("Destination", "Evidence"), rows),
        )
    page(
        "review.html",
        "Prepared candidate review",
        "<p>Review these prepared destinations and promote the same enclosing artifact bytes. "
        "Presence is not prose truth or approval.</p>"
        '<ul><li><a href="index.html">Contract Render and source-owned facts</a></li>'
        '<li><a href="documentation.html">Selected Markdown and guides</a></li>'
        '<li><a href="documentation-audit.html">Documentation Audit '
        "and authenticated baseline</a></li>"
        '<li><a href="../native-semantic-parity.json">Independent native semantic parity</a></li>'
        + "".join(directories)
        + "</ul>",
    )
    return pages


def prepared_qualification(
    candidate: Path,
    subjects: list[dict[str, Any]],
    code_qualification: Any,
    *,
    archived: bool = False,
) -> dict[str, Any] | None:
    """Qualify the artifact tuple after preparation; code-only evidence cannot substitute."""
    from .publication import verify_published_candidate

    build = cast(
        dict[str, Any],
        verify_published_candidate(candidate) if archived else verify_candidate(candidate),
    )
    if build["documentation"] is None:
        return None
    from .documentation_audit import check_record

    record = json.loads((candidate / "documentation-audit.json").read_bytes())
    if (
        record.get("format") != "riverhog-documentation-audit/v1"
        or record.get("stage") != "prepared"
        or record.get("state") not in {"PASS", "REVIEW"}
        or (not archived and check_record(record, prepared=True))
    ):
        raise ContractAtlasError(
            "prepared documentation FAIL/preview cannot enter release approval"
        )
    products = record["current"]["artifacts"]["products"]
    matched = []
    for subject in subjects:
        if subject["kind"] in {"wheel", "sdist"}:
            name = Path(subject["file"]).name
            if products.get(name) != subject["sha256"]:
                raise ContractAtlasError(
                    "release distribution differs from documentation-qualified bytes"
                )
            matched.append(subject["sha256"])
        elif subject["kind"] == "image":
            if "sha256:" + subject["sha256"] not in products.values():
                raise ContractAtlasError(
                    "release image differs from documentation-qualified ImageID"
                )
            matched.append(subject["sha256"])
        elif subject["kind"] == "source":
            if build["preparation"]["documented_source_archive_sha256"] != subject["sha256"]:
                raise ContractAtlasError("release source differs from exact documented preparation")
    return {
        "format": "riverhog-prepared-products-qualification/v1",
        "source_sha": build["source_sha"],
        "documentation": build["documentation"],
        "toolchain": build["toolchain"],
        "renderer": build["renderer"],
        "baseline": record["baseline_custody"],
        "code_qualification": code_qualification,
        "products": sorted(matched),
        "artifact_observations_sha256": canonical_sha256(record["current"]["artifacts"]),
        "review_manifest_sha256": build["files"]["riverhog-v1/manifest.json"],
        "build_manifest_sha256": canonical_sha256(build),
        "documentation_attention": record["state"],
    }
