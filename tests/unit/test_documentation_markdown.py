"""Exact, constrained front matter and native member ownership govern release authoring."""

import argparse
import copy
import importlib
import re
import sys
from pathlib import Path

import pytest

from tests.documentation_fixtures import PARAMETER, ROOT_TARGET, SCHEMA, closure, document, png

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from contract_atlas.documentation_markdown import (  # noqa: E402
    compile_corpus,
    front_matter,
    selected_content,
)
from contract_atlas.documentation_native import (  # noqa: E402
    apply_cli_prose,
    expected_outputs,
    extract_cli,
    native_slots,
    select_observations,
)
from contract_atlas.documentation_requirements import build_requirements, target_key  # noqa: E402
from contract_atlas.model import ContractAtlasError  # noqa: E402
from release_documentation_lib import annotate_openapi  # noqa: E402


@pytest.fixture
def requirements():
    return build_requirements(closure())


def compile_one(requirements, payload=None):
    return compile_corpus(
        {"reference.md": document() if payload is None else payload},
        requirements,
        routes={"cli:fixture": "cli.html", "schema:fixture": "schema.html"},
    )


def test_native_children_are_required_and_suppressed_inputs_keep_their_semantics(requirements):
    result = compile_one(requirements)
    assert SCHEMA in result["coverage"]["missing"]
    assert {"element_id": "schema:fixture", "pointer": "/properties/description"} in result[
        "coverage"
    ]["missing"]
    assert {"element_id": "schema:fixture", "pointer": "/properties/limit"} in result["coverage"][
        "missing"
    ]
    row = next(
        r
        for r in requirements["subjects"].values()
        if r["target"].get("member", {}).get("key") == "secret"
    )
    assert row["rule"] == "structural" and not row["destinations"]


def test_sections_follow_heading_hierarchy_without_implicit_child_coverage(requirements):
    result = compile_one(requirements)
    entry = result["resolved"][target_key(ROOT_TARGET)]
    assert "Exact detail." in selected_content(result, entry)["plain"]
    assert "Other section." not in selected_content(result, entry)["plain"]
    assert selected_content(result, result["resolved"][target_key(PARAMETER)]) == {}
    assert result == compile_one(requirements)


def test_moves_change_source_identity_without_changing_effective_prose(requirements):
    before = compile_one(requirements)
    after = compile_corpus({"other/reference.md": document()}, requirements)
    key = target_key(ROOT_TARGET)
    assert selected_content(before, before["resolved"][key]) == selected_content(
        after, after["resolved"][key]
    )
    assert before["source_files"] != after["source_files"]


@pytest.mark.parametrize(
    "header",
    [
        "id: one\nid: two",
        "subjects: &a [test]\ncopy: *a",
        "id: !!str reference",
        "id: reference\n<<: {kind: reference}",
        "? [a, b]\n: value",
        "%YAML 1.2\n---\nid: reference",
        "id: reference\n---\nother: document",
        "unexpected: field",
    ],
)
def test_unsafe_or_unknown_yaml_is_rejected(header):
    with pytest.raises(ContractAtlasError):
        front_matter(("---\n" + header + "\n---\nbody\n").encode())


@pytest.mark.parametrize(
    "body",
    [
        "<script>bad()</script>",
        "[bad](javascript:alert(1))",
        "![remote](https://example.invalid/x.png)",
        "[parent](../other.md)",
        "[missing](lost.md)",
        "[missing](#unknown)",
        "# Same\n\n# Same",
        "{{ template }}",
        "\x1b[31mcontrol",
        "[unsafe](file:///tmp/data)",
    ],
)
def test_unsafe_markup_links_and_anchors_are_rejected(requirements, body):
    with pytest.raises(ContractAtlasError):
        compile_one(requirements, document(body=body))


def test_code_fences_are_literal_and_not_directives(requirements):
    result = compile_one(
        requirements,
        document(
            body="# Main\n\n```text\n<script>{{ literal }}</script>\n!include ../outside\n```\n"
        ),
    )
    assert "&lt;script&gt;" in result["pages"]["reference.md"]["html"]
    assert "!include ../outside" in result["pages"]["reference.md"]["plain"]


def test_duplicate_ids_bindings_and_false_aliases_fail(requirements):
    with pytest.raises(ContractAtlasError, match="duplicate document ID"):
        compile_corpus({"one.md": document(), "two.md": document()}, requirements)
    with pytest.raises(ContractAtlasError, match="duplicate"):
        compile_one(requirements, document([{"target": ROOT_TARGET, "summary": "One."}] * 2))
    false_alias = {
        "element_id": "cli:fixture",
        "member": {"kind": "cli-parameter", "key": "invented"},
    }
    with pytest.raises(ContractAtlasError, match="unknown"):
        compile_one(requirements, document([{"target": false_alias, "summary": "Wrong."}]))


def test_unlinked_guides_do_not_satisfy_reference_requirements(requirements):
    result = compile_corpus(
        {"unlinked.md": document([SCHEMA], identifier="guide", kind="guide")}, requirements
    )
    assert result["guides"][0]["id"] == "guide"
    assert SCHEMA in result["coverage"]["missing"]


def test_empty_explicit_starters_stay_incomplete(requirements):
    result = compile_one(requirements, document([{"target": ROOT_TARGET, "summary": ""}], body=""))
    assert ROOT_TARGET in result["coverage"]["missing"]


def test_assets_and_subject_links_are_captured_and_checked(requirements):
    files = {
        "reference.md": document(
            body=(
                "# Main\n\n![Architecture](assets/figure.png)\n\n[Data](cont"
                "ract:schema%3Afixture)\n"
            )
        ),
        "assets/figure.png": png(),
    }
    result = compile_corpus(files, requirements, routes={"schema:fixture": "schema.html"})
    assert result["source_files"]["assets/figure.png"]["size"] == len(png())
    assert "documentation-assets/assets/figure.png" in result["pages"]["reference.md"]["html"]
    assert 'href="schema.html"' in result["pages"]["reference.md"]["html"]
    files["assets/figure.png"] += b"trailing payload"
    with pytest.raises(ContractAtlasError, match="trailing"):
        compile_corpus(files, requirements)


def test_argparse_literal_prose_does_not_change_parsing(requirements):
    compiled = compile_one(requirements)
    parser = argparse.ArgumentParser(prog="fixture", description="Developer context.")
    parser.add_argument("--archive", default="source")
    parser.add_argument("--secret", help=argparse.SUPPRESS)
    before = parser.parse_args(["--archive", "same", "--secret", "value"])
    apply_cli_prose({"fixture": parser}, compiled, requirements)
    assert parser.parse_args(["--archive", "same", "--secret", "value"]) == before
    help_text = parser.format_help()
    assert "50% %(prog)s {value} Résumé 東京." in help_text and "--secret" not in help_text
    expected = expected_outputs(native_slots(compiled, requirements))
    observed, readouts = extract_cli({"fixture": parser})
    assert select_observations(expected, observed) == {
        k: v for k, v in expected.items() if k.startswith("cli/")
    }
    assert readouts["fixture"] == help_text


def test_openapi_prose_cannot_rewrite_wire_description_fields_or_security():
    original = {
        "paths": {
            "/data": {
                "post": {
                    "operationId": "create",
                    "security": [{"Bearer": []}],
                    "requestBody": {"required": True},
                    "responses": {"200": {"description": "OK"}},
                }
            }
        },
        "components": {
            "schemas": {
                "Input": {
                    "properties": {"description": {"type": "string", "default": "wire value"}},
                    "additionalProperties": False,
                }
            }
        },
    }
    before = copy.deepcopy(original)
    result = annotate_openapi(
        original,
        {
            "/paths/~1data/post": {"summary": "Create data.", "markdown": "Exact body."},
            "/components/schemas/Input/properties/description": {
                "summary": "Wire field.",
                "markdown": "",
            },
        },
    )
    assert original == before
    assert (
        result["paths"]["/data"]["post"]["security"] == before["paths"]["/data"]["post"]["security"]
    )
    assert (
        result["components"]["schemas"]["Input"]["properties"]["description"]["default"]
        == "wire value"
    )
    assert result["paths"]["/data"]["post"]["summary"] == "Create data."


def test_native_markdown_retains_commonmark_structure_and_literal_text():
    from contract_atlas.documentation_markdown import native_markdown
    from markdown_it import MarkdownIt
    from release_documentation_lib import markdown_text

    parser = MarkdownIt("commonmark", {"html": False})
    source = (
        "# Heading\n\n3. Ordered item\n   - Nested **strong** item\n\n"
        "> Quoted *text*.\n>\n> Paragraph with two spaces  \n> after hard break.\n\n"
        '[External](https://example.invalid/path "Link title") and ` ` and ``a`b``.\n\n'
        "````text\n```\n<script>{{ literal }}</script>\n\n````\n"
        "\nLiteral &amp;amp; and \\[brackets\\].\n"
    )
    projected = native_markdown(parser.parse(source))
    assert parser.render(projected) == parser.render(source)
    summary = "<script>50% [brackets] &amp;</script>"
    assert "<script>" not in parser.render(markdown_text(summary))
    assert "&amp;amp;" in parser.render(markdown_text(summary))


def test_authoring_errors_identify_file_field_and_actual_line(requirements):
    payload = document().replace(b"kind: reference", b"unknown: value\nkind: reference")
    with pytest.raises(ContractAtlasError, match=r"reference.md: line 4: field unknown"):
        compile_one(requirements, payload)
    payload = document([{"target": PARAMETER, "summary": "Text.", "body": "#absent"}])
    with pytest.raises(
        ContractAtlasError, match=r"reference.md: line \d+: field subjects\[0\].body"
    ):
        compile_one(requirements, payload)
    payload = document([{"target": {**PARAMETER, "extra": "unowned"}, "summary": "Text."}])
    with pytest.raises(
        ContractAtlasError, match=r"reference.md: line \d+: field subjects\[0\]: target"
    ):
        compile_one(requirements, payload)


@pytest.mark.parametrize(
    ("before", "after", "field"),
    [
        (b"id: reference", b"id: INVALID", "id"),
        (b"kind: reference", b"kind: unsupported", "kind"),
        (b"title: Reference", b"title: []", "title"),
        (b"subjects:\n", b"subjects: []\nunrelated:\n", "unrelated"),
        (b"id: reference", b"id: reference\nid: duplicate", "id"),
        (b"id: reference", b"id: &anchor reference", "front matter"),
    ],
)
def test_invalid_front_matter_reports_the_exact_file_and_field(requirements, before, after, field):
    raw = document()
    assert before in raw
    with pytest.raises(
        ContractAtlasError, match=rf"reference.md: line \d+: field {re.escape(field)}:"
    ):
        compile_one(requirements, raw.replace(before, after))


@pytest.mark.parametrize("mutation", ["waiver", "prose", "capture"])
def test_current_verification_rederives_requirements_and_prose(mutation):
    from contract_atlas.documentation import AuthoredDocumentation, verify_compilation
    from contract_atlas.documentation_requirements import target_key

    code = closure()
    files = {"reference.md": document()}
    compiled, requirements = AuthoredDocumentation("v1.0.0", "b" * 40, files).compile(code)
    record = {"compiled": compiled, "requirements": requirements}
    verify_compilation(code, files, record)
    if mutation == "waiver":
        record["requirements"]["subjects"][target_key(SCHEMA)]["rule"] = "structural"
    elif mutation == "prose":
        record["compiled"]["resolved"][target_key(ROOT_TARGET)]["summary"] = "Invented prose."
    else:
        files["reference.md"] = files["reference.md"].replace(
            b"Archive to recover.", b"Changed prose."
        )
    with pytest.raises(ContractAtlasError, match="native source policy|captured Markdown"):
        verify_compilation(code, files, record)


@pytest.mark.parametrize(
    ("module", "name", "parameter"),
    [
        ("http_api_contracts", "HttpResponseHeaderContract", "value_type"),
        ("http_api_contracts", "BrowseTokenCodec", "clock"),
        ("gogurt_core", "iter_new_mounts", "sleep"),
    ],
)
def test_native_defaults_do_not_hide_meaningful_python_parameters(module, name, parameter):
    import contract_freeze

    value = getattr(importlib.import_module(module), name)
    code = closure()
    identity = module + "." + name
    pointer = "/external_contract/python_reference"
    code["external_contract"]["python_reference"] = {
        "module": module,
        "name": name,
        "unit": "export",
        "distribution": "fixture",
        "contract": contract_freeze._python_export(value),
    }
    code["elements"].append(
        {
            "id": "python:" + identity,
            "title": identity,
            "authority": "fixture",
            "interface": "python",
            "pointers": [pointer],
            "related_element_ids": [],
        }
    )
    requirements = build_requirements(code)
    key = target_key(
        {
            "element_id": "python:" + identity,
            "member": {"kind": "python-parameter", "key": parameter},
        }
    )
    assert requirements["subjects"][key]["rule"] in {"authored", "reference"}
    assert requirements["subjects"][key]["meaning"]["owned"] in requirements["scopes"]
