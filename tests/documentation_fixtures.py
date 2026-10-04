"""Small source-owned documentation inputs; never a production release corpus."""

import struct
import zlib

import yaml

SOURCE = "riverhog-release-documentation-document/v1"
ROOT_TARGET = {"element_id": "cli:fixture", "pointer": ""}
PARAMETER = {"element_id": "cli:fixture", "member": {"kind": "cli-parameter", "key": "archive"}}
SCHEMA = {"element_id": "schema:fixture", "pointer": ""}


class AuthoringDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def closure():
    return {
        "series": "v1",
        "boundaries": {},
        "unsafe_integer_paths": [],
        "external_contract": {
            "release": {"compatibility": {}},
            "extents": {},
            "cli": {
                "fixture": {
                    "name": "fixture",
                    "parameters": [
                        {"dest": "archive", "options": ["--archive"], "default": "source"},
                        {
                            "dest": "secret",
                            "options": ["--secret"],
                            "help_visibility": "suppressed",
                        },
                    ],
                }
            },
            "schemas": {
                "test": {
                    "type": "object",
                    "properties": {
                        "description": {"type": "string"},
                        "limit": {"type": "integer", "default": 4},
                    },
                }
            },
        },
        "elements": [
            {
                "id": "cli:fixture",
                "authority": "fixture",
                "interface": "cli",
                "title": "fixture",
                "pointers": [
                    "/external_contract/cli/fixture/name",
                    "/external_contract/cli/fixture/parameters",
                ],
                "related_element_ids": [],
            },
            {
                "id": "schema:fixture",
                "authority": "fixture",
                "interface": "schema",
                "title": "Data",
                "pointers": ["/external_contract/schemas/test"],
                "related_element_ids": [],
            },
        ],
    }


def document(
    entries=None,
    body="# Main\n\nFirst section.\n\n## Detail\n\nExact detail.\n\n# Next\n\nOther section.\n",
    *,
    identifier="reference",
    kind="reference",
):
    subjects = (
        entries
        if entries is not None
        else [
            {
                "target": ROOT_TARGET,
                "summary": "Use literal 50% %(prog)s {value} Résumé 東京.",
                "body": "#main",
            },
            {"target": PARAMETER, "summary": "Archive to recover."},
        ]
    )
    header = {
        "format": SOURCE,
        "id": identifier,
        "kind": kind,
        "title": "Reference",
        "subjects": subjects,
    }
    return (
        "---\n"
        + yaml.dump(header, Dumper=AuthoringDumper, sort_keys=False, allow_unicode=True)
        + "---\n"
        + body
    ).encode()


def png():
    def chunk(kind, data):
        return (
            struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\0\0\0\0"))
        + chunk(b"IEND", b"")
    )


def synthetic_corpus(code):
    """Complete machine-test prose, explicitly not the authored v1 release corpus."""
    from contract_atlas.documentation_requirements import build_requirements

    rows = [
        row for row in build_requirements(code)["subjects"].values() if row["rule"] == "authored"
    ]
    return {
        f"reference/native-{index // 80}.md": document(
            [
                {
                    "target": row["target"],
                    "summary": "Synthetic reference for " + row["title"] + ".",
                    "body": "#",
                }
                for row in rows[index : index + 80]
            ],
            body="# Synthetic reference\n\nTest-only prose for machinery qualification.\n",
            identifier=f"synthetic-native-{index // 80}",
        )
        for index in range(0, len(rows), 80)
    }


def prepared_readout_model(code, slots, expected, authored):
    """Custody/Pages models; actual built-destination proof lives in native product tests."""
    import copy

    from contract_atlas.documentation import source_ledger
    from contract_atlas.documentation_artifacts import native_request, source_native_semantics
    from contract_atlas.model import canonical_bytes, canonical_sha256
    from release_documentation_lib import annotate_openapi

    plan = {
        "slots": slots,
        "source_capture": {
            "commit": authored.commit,
            "tag": authored.tag,
            "files": source_ledger(authored.files),
        },
    }
    request = native_request(code, plan)
    source = source_native_semantics(code, request)
    semantics = copy.deepcopy(source)
    semantics["openapi"] = {
        name: {**value, "value": annotate_openapi(value["value"], slots["openapi"].get(name, {}))}
        for name, value in source["openapi"].items()
    }
    actual = {
        "observed": {k: v for k, v in expected.items() if not k.startswith("oci/")},
        "readouts": {
            "cli": {name: "Synthetic installed help model." for name in slots["cli"]},
            "python": {name: expected["python/" + name] for name in slots["python"]},
            "openapi": semantics["openapi"],
            "metadata": {
                name: f"Metadata-Version: 2.4\nName: {name}\nSummary: {value['summary']}\n\n"
                + expected["metadata/" + name]["description"]
                + "\n"
                for name, value in slots["metadata"].items()
            },
        },
        "semantics": semantics,
        "installed_modules": sorted(request["modules"]),
        "cli_journeys": {
            name: {"argv": [name, "--help"], "exit": 0, "stdout": "Synthetic installed help model."}
            for name in source["cli"]["value"]
        },
    }
    oci = {
        name: {
            "image_id": "sha256:" + canonical_sha256(name),
            "labels": {"org.opencontainers.image.description": expected["oci/" + name]},
        }
        for name in slots["oci"]
    }
    from contract_atlas.documentation_artifacts import verify_readout_semantics

    parity = verify_readout_semantics(source, semantics, request)
    raw = {
        "documentation-preparation-plan.json": canonical_bytes(plan),
        "native-source-semantics.json": canonical_bytes(source),
        "native-readouts.json": canonical_bytes(actual),
        "native-oci.json": canonical_bytes(oci),
        "native-semantic-parity.json": canonical_bytes(parity),
    }
    artifacts = {
        "products": {"oci/" + name: value["image_id"] for name, value in oci.items()},
        "readouts": {
            name: canonical_sha256(value)
            for name, value in (
                ("native-readouts.json", actual),
                ("native-oci.json", oci),
                ("native-semantic-parity.json", parity),
            )
        },
        "semantic_parity": parity,
        "fixture": "synthetic-model-only",
    }
    return raw, artifacts
