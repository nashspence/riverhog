"""Isolated #952 contract tests; synthetic fixtures are not actual release docs."""
from __future__ import annotations

import argparse
import copy
import inspect
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from compiler import (SOURCE, Invalid, annotate_openapi, annotate_python, apply_argparse,
                      capture_git, compile_corpus, digest, proposed_review, report_bytes,
                      stage_project_metadata, target)


def root(name="command"):
    return {"element_id": name, "pointer": ""}


def parameter(name="count"):
    return {"element_id": "command", "member": {"kind": "cli-parameter", "key": name}}


def ledger():
    return {"format": "riverhog-doc-requirements-reference/v1", "closure_sha256": "a" * 64,
            "policy_sha256": "b" * 64, "complete": True, "subjects": [
        {"target": root(), "interface": "cli", "authority": "app", "rule": "authored", "detail": True},
        {"target": parameter(), "interface": "cli", "authority": "app", "rule": "authored", "detail": False},
        {"target": root("count-type"), "interface": "cli", "authority": "app", "rule": "structural", "detail": False}]}


def corpus():
    return {"documentation.json": report_bytes({"format": SOURCE, "entries": [
        {"target": root(), "summary": "Recover one object.", "body": "reference/recover.md"},
        {"target": parameter(), "summary": "Recover 100% of the selected count; %(default)s is literal."}],
        "guides": [{"id": "recover", "title": "Recover an object", "body": "guides/recover.md", "subjects": [root()]}]}),
        "reference/recover.md": b"# Recovery\n\nUse the generated syntax. See [guide](../guides/recover.md#steps).\n",
        "guides/recover.md": b"# Steps\n\n1. Select an object.\n2. Follow [reference](../reference/recover.md#recovery).\n"}


def edited(files, edit):
    result = copy.deepcopy(files)
    doc = json.loads(result["documentation.json"])
    edit(doc)
    result["documentation.json"] = report_bytes(doc)
    return result


def complete():
    files, req = corpus(), ledger()
    files["review.json"] = proposed_review(compile_corpus(files, req))
    return files, req


class SchemaTests(unittest.TestCase):
    def test_schema_matches_reference_corpus_and_rejects_unknown_fields(self):
        from jsonschema import Draft202012Validator
        schema = json.loads(Path(__file__).with_name("source.schema.json").read_bytes())
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        value = json.loads(corpus()["documentation.json"])
        self.assertFalse(list(validator.iter_errors(value)))
        value["entries"][0]["default"] = 42
        self.assertTrue(list(validator.iter_errors(value)))


class CompilationTests(unittest.TestCase):
    def test_preview_and_explicit_review(self):
        files, req = corpus(), ledger()
        before = copy.deepcopy(files)
        result = compile_corpus(files, req)
        self.assertFalse(result["review_matches"])
        self.assertEqual(result["coverage"], {"total": 3, "missing": [], "structural": 1})
        self.assertEqual(files, before)
        with self.assertRaisesRegex(Invalid, "review"):
            compile_corpus(files, req, final=True)
        files["review.json"] = proposed_review(result)
        self.assertTrue(compile_corpus(files, req, final=True)["review_matches"])

    def test_exact_bytes_separate_from_normalized_record(self):
        files, req = complete()
        result = compile_corpus(files, req, final=True)
        self.assertEqual(result["source_files"]["guides/recover.md"], digest(files["guides/recover.md"]))
        self.assertIn("review.json", result["source_files"])
        self.assertNotIn("review.json", result["files"])
        self.assertIn('href="../guides/recover.html#steps"', result["pages"]["reference/recover.md"]["html"])

    def test_missing_child_is_not_covered_by_parent_or_guide(self):
        files = edited(corpus(), lambda d: d["entries"].pop())
        result = compile_corpus(files, ledger())
        self.assertEqual(result["coverage"]["missing"], [parameter()])
        with self.assertRaises(Invalid): proposed_review(result)

    def test_details_required_by_source_policy(self):
        files = edited(corpus(), lambda d: d["entries"][0].pop("body"))
        # The guide still reaches the reference page; its mere existence is not a target binding.
        self.assertIn(root(), compile_corpus(files, ledger())["coverage"]["missing"])

    def test_empty_corpus_cannot_pass_final(self):
        files = {"documentation.json": report_bytes({"format": SOURCE, "entries": [], "guides": []})}
        self.assertEqual(len(compile_corpus(files, ledger())["coverage"]["missing"]), 2)
        with self.assertRaises(Invalid): compile_corpus(files, ledger(), final=True)

    def test_unknown_target_and_waiver_rejected(self):
        for edit in [lambda d: d["entries"][0].update(target=root("unknown")),
                     lambda d: d["entries"][0].update(self_describing=True),
                     lambda d: d.update(package_inventory=[]),
                     lambda d: d["entries"][0].update(default=42)]:
            with self.subTest(edit=edit), self.assertRaises(Invalid):
                compile_corpus(edited(corpus(), edit), ledger())

    def test_duplicate_entry_and_json_key_rejected(self):
        files = edited(corpus(), lambda d: d["entries"].append(d["entries"][0]))
        with self.assertRaises(Invalid): compile_corpus(files, ledger())
        files = corpus(); files["documentation.json"] = b'{"format":"x","format":"y"}'
        with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_unknown_or_incomplete_source_policy_rejected(self):
        for mutate in [lambda r: r.update(complete=False),
                       lambda r: r["subjects"][0].update(interface="new-interface"),
                       lambda r: r["subjects"][0].update(rule="self-describing")]:
            req = ledger(); mutate(req)
            with self.assertRaises(Invalid): compile_corpus(corpus(), req)

    def test_named_parameter_binding_does_not_depend_on_ledger_order(self):
        req = ledger(); req["subjects"].reverse()
        result = compile_corpus(corpus(), req)
        self.assertEqual(result["resolved"][target(parameter())]["summary"],
                         compile_corpus(corpus(), ledger())["resolved"][target(parameter())]["summary"])

    def test_canonical_source_reference_reuses_real_document(self):
        req = ledger(); req["subjects"].append({"target": root("alias"), "interface": "cli", "authority": "app",
            "rule": "reference", "detail": True, "canonical": root()})
        result = compile_corpus(corpus(), req)
        self.assertFalse(result["coverage"]["missing"])
        self.assertEqual(result["resolved"][target(root("alias"))], result["resolved"][target(root())])

    def test_author_cannot_invent_canonical_alias(self):
        files = edited(corpus(), lambda d: d["entries"][0].update(use=root("other")))
        with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_canonical_cycles_and_structural_donors_rejected(self):
        for donor in (root("alias"), root("count-type")):
            req = ledger(); req["subjects"].append({"target": root("alias"), "interface": "cli", "authority": "app",
                "rule": "reference", "detail": False, "canonical": donor})
            with self.assertRaises(Invalid): compile_corpus(corpus(), req)

    def test_reference_to_missing_document_reports_unresolved(self):
        req = ledger(); req["subjects"].append({"target": root("alias"), "interface": "cli", "authority": "app",
                "rule": "reference", "detail": False, "canonical": parameter()})
        result = compile_corpus(edited(corpus(), lambda d: d["entries"].pop()), req)
        self.assertIn(root("alias"), result["coverage"]["missing"])

    def test_stale_meaning_policy_prose_all_invalidate_review(self):
        for change in ("meaning", "policy", "prose", "formatting"):
            files, req = complete()
            if change == "meaning": req["closure_sha256"] = "c" * 64
            elif change == "policy": req["policy_sha256"] = "d" * 64
            elif change == "prose": files["guides/recover.md"] += b"\nNew explanation.\n"
            else: files["documentation.json"] += b"\n"
            with self.subTest(change=change), self.assertRaisesRegex(Invalid, "review"):
                compile_corpus(files, req, final=True)

    def test_prose_change_does_not_modify_source_semantic_identity(self):
        req = ledger(); files = corpus(); first = compile_corpus(files, req)
        files["guides/recover.md"] += b"\nExtra explanation.\n"
        second = compile_corpus(files, req)
        self.assertEqual(first["review_expected"]["closure_sha256"], second["review_expected"]["closure_sha256"])
        self.assertNotEqual(first["review_expected"]["corpus_sha256"], second["review_expected"]["corpus_sha256"])

    def test_build_is_repeatable_and_never_refreshes_review(self):
        files, req = complete(); before = copy.deepcopy(files)
        self.assertEqual(report_bytes(compile_corpus(files, req, final=True)), report_bytes(compile_corpus(files, req, final=True)))
        self.assertEqual(files, before)


class MarkdownTests(unittest.TestCase):
    def test_broken_fragment_and_missing_page_rejected(self):
        for body in (b"# Steps\n[broken](missing.md)", b"# Steps\n[bad](../reference/recover.md#missing)"):
            files = corpus(); files["guides/recover.md"] = body
            with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_html_and_unsafe_uris_rejected(self):
        for body in (b"# Steps\n<script>alert(1)</script>", b"# Steps\n[x](javascript:alert)",
                     b"# Steps\n[x](file:///tmp/x)", b"# Steps\n[x](../../outside.md)",
                     b"# Steps\n![x](https://example.com/image.png)"):
            files = corpus(); files["guides/recover.md"] = body
            with self.subTest(body=body), self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_code_fence_is_data_and_unicode_supported(self):
        files = corpus(); files["guides/recover.md"] += '\n```text\n<script>literal</script>\n```\n\nCafé ✓\n'.encode()
        page = compile_corpus(files, ledger())["pages"]["guides/recover.md"]
        self.assertIn("&lt;script&gt;", page["html"])
        self.assertIn("Café", page["plain"])

    def test_unsafe_terminal_controls_rejected(self):
        files = corpus(); files["guides/recover.md"] += b"\x1b[31m"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_duplicate_headings_and_orphan_files_rejected(self):
        files = corpus(); files["guides/recover.md"] += b"\n# Steps\n"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())
        files = corpus(); files["orphan.md"] = b"Unused"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_case_collision_and_body_escape_rejected(self):
        files = corpus(); files["Reference/Recover.md"] = b"Wrong case"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())
        files = edited(corpus(), lambda d: d["entries"][0].update(body="../other/release.md"))
        with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_contract_links_require_real_subject_and_source_route(self):
        files = corpus(); files["guides/recover.md"] += b"\n[command](contract:command)\n"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())
        result = compile_corpus(files, ledger(), routes={"command": "/riverhog/v1.0.0/command/"})
        self.assertIn('/riverhog/v1.0.0/command/', result["pages"]["guides/recover.md"]["html"])
        files["guides/recover.md"] += b"\n[absent](contract:absent)\n"
        with self.assertRaises(Invalid): compile_corpus(files, ledger(), routes={"command": "/x"})


class NativeProjectionTests(unittest.TestCase):
    def setUp(self):
        self.result = compile_corpus(corpus(), ledger())
        self.command = self.result["resolved"][target(root())]
        self.option = self.result["resolved"][target(parameter())]

    def test_argparse_real_help_literal_percent_and_unchanged_parsing(self):
        parser = argparse.ArgumentParser(description="Development context")
        parser.add_argument("--count", type=int, default=3)
        before = vars(parser.parse_args(["--count", "7"]))
        apply_argparse(parser, self.command, {"count": self.option})
        self.assertEqual(vars(parser.parse_args(["--count", "7"])), before)
        self.assertEqual(parser.parse_args([]).count, 3)
        rendered = parser.format_help()
        self.assertIn("Recover one object.", rendered)
        self.assertIn("100%", rendered)
        self.assertIn("%(default)s is literal", rendered)

    def test_unknown_or_suppressed_cli_slot_rejected(self):
        parser = argparse.ArgumentParser(description="Development context")
        parser.add_argument("--hidden", help=argparse.SUPPRESS)
        for name in ("absent", "hidden"):
            with self.assertRaises(Invalid): apply_argparse(parser, self.command, {name: self.option})
        self.assertEqual(parser.description, "Development context")

    def test_openapi_annotations_do_not_change_wire_description_field(self):
        spec = {"paths": {"/items": {"get": {"operationId": "items", "responses": {
            "200": {"description": "Success", "content": {"application/json": {"schema": {
                "type": "object", "properties": {"description": {"type": "string", "default": "wire-value"}}}}}}}}}}}
        before = copy.deepcopy(spec)
        after = annotate_openapi(spec, {("/items", "get"): self.command})
        self.assertEqual(spec, before)
        self.assertEqual(after["paths"]["/items"]["get"]["responses"], before["paths"]["/items"]["get"]["responses"])
        self.assertEqual(after["paths"]["/items"]["get"]["summary"], self.command["summary"])
        with self.assertRaises(Invalid): annotate_openapi(spec, {("/missing", "get"): self.command})

    def test_python_docstring_without_wrapper_or_signature_change(self):
        def example(value: int = 2) -> int: return value + 1
        signature = inspect.signature(example)
        identity = id(example)
        self.assertIs(annotate_python(example, self.command, owned_module=__name__), example)
        self.assertEqual(id(example), identity)
        self.assertEqual(inspect.signature(example), signature)
        self.assertEqual(example(), 3)
        self.assertIn("Recover one object.", inspect.getdoc(example))
        with self.assertRaises(Invalid): annotate_python(len, self.command, owned_module=__name__)

    def test_prepared_metadata_preserves_technical_legal_fields(self):
        project = {"name": "example", "version": "1.0.0", "license": "Apache-2.0",
                   "dependencies": ["dependency>=1"], "description": "Development only"}
        updated = stage_project_metadata(project, self.command)
        self.assertEqual(project["description"], "Development only")
        for key in ("name", "version", "license", "dependencies"): self.assertEqual(updated[key], project[key])
        self.assertEqual(updated["description"], self.command["summary"])
        with self.assertRaises(Invalid): stage_project_metadata({"dynamic": ["description"]}, self.command)


class GitCaptureTests(unittest.TestCase):
    def test_exact_committed_markdown_not_dirty_worktree(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            def git(*args): return subprocess.check_output(["git", "-C", temp, *args], stderr=subprocess.PIPE).decode().strip()
            git("init", "-b", "release-documentation")
            git("config", "user.name", "Synthetic Test"); git("config", "user.email", "test@example.invalid")
            for name, payload in corpus().items():
                p = repo / "v1.0.0" / name; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(payload)
            git("add", "."); git("commit", "-m", "Synthetic authored corpus")
            sha = git("rev-parse", "HEAD")
            (repo / "v1.0.0/guides/recover.md").write_text("Dirty change")
            self.assertEqual(capture_git(temp, sha, "v1.0.0"), corpus())
            with self.assertRaises(Invalid): capture_git(temp, "main", "v1.0.0")
            (repo / "v1.0.0/bad.md").symlink_to("../../outside")
            git("add", "v1.0.0/bad.md"); git("commit", "-m", "Synthetic bad symlink")
            with self.assertRaises(Invalid): capture_git(temp, git("rev-parse", "HEAD"), "v1.0.0")


if __name__ == "__main__": unittest.main()
