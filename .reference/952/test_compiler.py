"""Reference-only witnesses; synthetic subjects and prose are not a release corpus."""
from __future__ import annotations

import argparse
import copy
import inspect
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml
from compiler import (SOURCE, Invalid, annotate_openapi, annotate_python, apply_argparse,
                      capture_git, compile_corpus, digest, documentation_plan, front_matter,
                      report_bytes, stage_project_metadata, target)


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


def document(meta, body):
    return ("---\n" + yaml.safe_dump(meta, sort_keys=False, allow_unicode=True) + "---\n" + body).encode()


def corpus():
    return {"reference/recover.md": document({"format": SOURCE, "id": "recovery-reference", "kind": "reference",
        "title": "Recovery", "subjects": [
        {"target": root(), "summary": "Recover one object.", "body": "#"},
        {"target": parameter(), "summary": "Recover 100% of the selected count; %(default)s is literal."}]},
        "# Recovery\n\nUse the generated syntax. See [guide](../guides/recover.md#steps).\n"),
        "guides/recover.md": document({"format": SOURCE, "id": "recovery-guide", "kind": "guide", "title": "Recover an object",
        "subjects": [root()]}, "# Steps\n\n1. Select an object.\n2. Follow [reference](../reference/recover.md#recovery).\n")}


def edited(files, edit, name="reference/recover.md"):
    result = dict(files); meta, body = front_matter(result[name]); edit(meta)
    result[name] = document(meta, body)
    return result


def body_edit(files, body, name="guides/recover.md"):
    result = dict(files); meta, _ = front_matter(result[name]); result[name] = document(meta, body)
    return result


class FrontMatterTests(unittest.TestCase):
    def test_authoring_guide_examples_compile(self):
        from markdown_it import MarkdownIt
        source = Path(__file__).with_name("AUTHORING.md").read_text()
        blocks = [t.content.encode() for t in MarkdownIt().parse(source)
                  if t.type == "fence" and t.info == "markdown"]
        self.assertEqual(len(blocks), 2)
        files = dict(zip(("reference/recover.md", "guides/recover.md"), blocks, strict=True))
        self.assertFalse(compile_corpus(files, ledger(), final=True)["coverage"]["missing"])

    def test_schema_matches_parsed_reference_and_guide(self):
        from jsonschema import Draft202012Validator
        schema = json.loads(Path(__file__).with_name("source.schema.json").read_bytes())
        Draft202012Validator.check_schema(schema); validator = Draft202012Validator(schema)
        for data in corpus().values():
            meta, _ = front_matter(data); self.assertFalse(list(validator.iter_errors(meta)))
        meta["extra"] = "no"; self.assertTrue(list(validator.iter_errors(meta)))

    def test_front_matter_is_not_rendered(self):
        page = compile_corpus(corpus(), ledger())["pages"]["reference/recover.md"]
        self.assertNotIn(SOURCE, page["html"]); self.assertNotIn("subjects:", page["plain"])

    def test_duplicate_merge_alias_tag_and_directive_are_rejected(self):
        original = corpus()["reference/recover.md"]
        for prefix in ("id: duplicate\n", "<<: ignored\n", "other: &a value\n", "other: *a\n",
                       "other: !!str value\n", "%YAML 1.2\n"):
            bad = original.replace(b"---\n", ("---\n" + prefix).encode(), 1)
            with self.subTest(prefix=prefix), self.assertRaises(Invalid): front_matter(bad)

    def test_plain_scalars_are_strings_not_implicit_yaml_values(self):
        for value in ("on", "false", "0012", "2026-10-03"):
            original = corpus()["reference/recover.md"].replace(b"title: Recovery", ("title: " + value).encode())
            self.assertEqual(front_matter(original)[0]["title"], value)

    def test_invalid_delimiters_utf8_and_nesting_rejected(self):
        for value in (b"# no metadata", b"---\nformat: x", b"\xff", b"---\nx: " + b"["*40 + b"v" + b"]"*40 + b"\n---\nx"):
            with self.assertRaises(Invalid): front_matter(value)

    def test_no_authored_json_index_or_review_file(self):
        for name in ("documentation.json", "review.json", "corpus.toml"):
            files = corpus(); files[name] = b"{}"
            with self.assertRaisesRegex(Invalid, "unsupported file"): compile_corpus(files, ledger())


class CompilationTests(unittest.TestCase):
    def test_complete_candidate_has_no_review_stamp(self):
        files = corpus(); before = copy.deepcopy(files)
        result = compile_corpus(files, ledger(), final=True)
        self.assertEqual(result["coverage"], {"total": 3, "missing": [], "structural": 1})
        self.assertEqual(files, before); self.assertNotIn("review_matches", result)
        self.assertEqual(report_bytes(result), report_bytes(compile_corpus(files, ledger(), final=True)))

    def test_exact_bytes_separate_from_normalized_record(self):
        files = corpus(); result = compile_corpus(files, ledger())
        self.assertEqual(result["source_files"]["guides/recover.md"], digest(files["guides/recover.md"]))
        self.assertIn('href="../guides/recover.html#doc-steps"', result["pages"]["reference/recover.md"]["html"])

    def test_missing_child_is_not_covered_by_parent_or_guide(self):
        files = edited(corpus(), lambda d: d["subjects"].pop())
        result = compile_corpus(files, ledger()); self.assertEqual(result["coverage"]["missing"], [parameter()])
        with self.assertRaises(Invalid): compile_corpus(files, ledger(), final=True)
        plan = documentation_plan(files, ledger())
        self.assertEqual([x["target"] for x in plan if x["status"] == "missing"], [parameter()])

    def test_detail_requires_explicit_body_not_page_presence(self):
        files = edited(corpus(), lambda d: d["subjects"][0].pop("body"))
        self.assertIn(root(), compile_corpus(files, ledger())["coverage"]["missing"])

    def test_section_projection_does_not_leak_other_sections(self):
        files = body_edit(corpus(), "# Recovery\nIntro\n## Count\nOnly count.\n## Other\nOther text.\n", "reference/recover.md")
        files = edited(files, lambda d: d["subjects"][1].update(body="#count"))
        content = compile_corpus(files, ledger())["resolved"][target(parameter())]["content"]
        self.assertIn("Only count.", content["plain"]); self.assertNotIn("Other text.", content["plain"])

    def test_missing_or_cross_file_body_selector_rejected(self):
        for value in ("#absent", "another.md#steps"):
            with self.assertRaises(Invalid): compile_corpus(edited(corpus(), lambda d: d["subjects"][0].update(body=value)), ledger())

    def test_empty_preview_reports_obligations_but_cannot_pass_final(self):
        self.assertEqual(len(compile_corpus({}, ledger())["coverage"]["missing"]), 2)
        with self.assertRaises(Invalid): compile_corpus({}, ledger(), final=True)

    def test_unknown_target_and_author_waiver_rejected(self):
        for edit in (lambda d: d["subjects"][0].update(target=root("absent")),
                     lambda d: d["subjects"][0].update(self_describing="true"),
                     lambda d: d.update(package_inventory=[]), lambda d: d["subjects"][0].update(default="42")):
            with self.assertRaises(Invalid): compile_corpus(edited(corpus(), edit), ledger())

    def test_duplicate_subject_or_document_id_rejected(self):
        with self.assertRaises(Invalid): compile_corpus(edited(corpus(), lambda d: d["subjects"].append(copy.deepcopy(d["subjects"][0]))), ledger())
        files = corpus(); files["duplicate.md"] = files["guides/recover.md"]
        with self.assertRaisesRegex(Invalid, "document ID"): compile_corpus(files, ledger())

    def test_unlinked_valid_document_is_included_not_silently_dropped(self):
        files = corpus(); meta, body = front_matter(files["guides/recover.md"]); meta["id"] = "another-guide"
        files["guides/extra.md"] = document(meta, body)
        self.assertIn("guides/extra.md", compile_corpus(files, ledger())["pages"])

    def test_file_rename_preserves_binding_but_changes_captured_identity(self):
        files = corpus(); before = compile_corpus(files, ledger())
        files["reference/renamed.md"] = files.pop("reference/recover.md")
        files["guides/recover.md"] = files["guides/recover.md"].replace(b"reference/recover.md", b"reference/renamed.md")
        after = compile_corpus(files, ledger())
        self.assertEqual(before["resolved"][target(root())]["document_id"], after["resolved"][target(root())]["document_id"])
        self.assertNotEqual(before["inputs"]["corpus_sha256"], after["inputs"]["corpus_sha256"])

    def test_source_policy_rejects_unknown_or_incomplete_classification(self):
        for mutate in (lambda r: r.update(complete=False), lambda r: r["subjects"][0].update(interface="new"),
                       lambda r: r["subjects"][0].update(rule="self-describing")):
            req = ledger(); mutate(req)
            with self.assertRaises(Invalid): compile_corpus(corpus(), req)

    def test_named_parameter_binding_independent_of_ledger_order(self):
        req = ledger(); req["subjects"].reverse()
        self.assertEqual(compile_corpus(corpus(), req)["resolved"], compile_corpus(corpus(), ledger())["resolved"])

    def test_real_canonical_reuse_not_invented_alias(self):
        req = ledger(); req["subjects"].append({"target": root("alias"), "interface": "cli", "authority": "app",
            "rule": "reference", "detail": True, "canonical": root()})
        result = compile_corpus(corpus(), req)
        self.assertEqual(result["resolved"][target(root("alias"))], result["resolved"][target(root())])
        with self.assertRaises(Invalid): compile_corpus(edited(corpus(), lambda d: d["subjects"][0].update(use=root("other"))), req)

    def test_canonical_cycles_and_structural_donors_rejected(self):
        for donor in (root("alias"), root("count-type")):
            req = ledger(); req["subjects"].append({"target": root("alias"), "interface": "cli", "authority": "app",
                "rule": "reference", "detail": False, "canonical": donor})
            with self.assertRaises(Invalid): compile_corpus(corpus(), req)

    def test_prose_and_formatting_change_input_identity_not_closure(self):
        files = corpus(); before = compile_corpus(files, ledger()); files["guides/recover.md"] += b"\n"
        after = compile_corpus(files, ledger())
        self.assertEqual(before["inputs"]["closure_sha256"], after["inputs"]["closure_sha256"])
        self.assertNotEqual(before["inputs"]["corpus_sha256"], after["inputs"]["corpus_sha256"])


class MarkdownTests(unittest.TestCase):
    def test_broken_local_links_and_fragments_rejected(self):
        for body in ("# Steps\n[broken](missing.md)", "# Steps\n[bad](../reference/recover.md#missing)"):
            with self.assertRaises(Invalid): compile_corpus(body_edit(corpus(), body), ledger())

    def test_html_unsafe_links_and_controls_rejected(self):
        for body in ("# Steps\n<script>alert(1)</script>", "# Steps\n[x](javascript:alert)",
                     "# Steps\n[x](file:///tmp/x)", "# Steps\n[x](../../outside.md)",
                     "# Steps\n![x](https://example.com/image.png)", "# Steps\n\x1b[31m"):
            with self.subTest(body=body), self.assertRaises(Invalid): compile_corpus(body_edit(corpus(), body), ledger())

    def test_fenced_data_and_unicode_not_executed_or_interpreted(self):
        files = corpus(); files["guides/recover.md"] += '\n```text\n---\n<script>literal</script>\n```\n\nCafé ✓\n'.encode()
        page = compile_corpus(files, ledger())["pages"]["guides/recover.md"]
        self.assertIn("&lt;script&gt;", page["html"]); self.assertIn("Café", page["plain"])

    def test_duplicate_headings_and_missing_front_matter_rejected(self):
        files = corpus(); files["guides/recover.md"] += b"\n# Steps\n"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())
        files = corpus(); files["orphan.md"] = b"Unindexed text"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_case_collision_rejected(self):
        files = corpus(); files["Reference/Recover.md"] = files["reference/recover.md"]
        with self.assertRaises(Invalid): compile_corpus(files, ledger())

    def test_contract_links_need_source_owned_routes(self):
        files = corpus(); files["guides/recover.md"] += b"\n[command](contract:command)\n"
        with self.assertRaises(Invalid): compile_corpus(files, ledger())
        page = compile_corpus(files, ledger(), routes={"command": "/riverhog/v1.0.0/command/"})["pages"]["guides/recover.md"]
        self.assertIn('/riverhog/v1.0.0/command/', page["html"])


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
