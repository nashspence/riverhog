from __future__ import annotations

import contextlib
import importlib.util
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("riverhog_guidance", ROOT / "scripts/guidance.py")
assert SPEC is not None and SPEC.loader is not None
guidance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = guidance
SPEC.loader.exec_module(guidance)

HEADER = 'schema = "riverhog-nonbinding-guidance/v1"\n'
ENTRY = '''
[[heuristics]]
id = "prefer-one-result"
guidance = "Prefer sharing one result."
scope = "Comparable presentations."
rationale = "Avoid accidental divergence."
[[heuristics.related_tests]]
node = "tests/test_example.py::test_result"
note = "A narrow example, not a general guarantee."
'''
EMPTY_TEST_ENTRY = '''
[[heuristics]]
id = "prefer-one-result"
guidance = "Consider one semantic owner."
scope = "Design review."
rationale = "This is an unautomated design preference."
related_tests = []
'''


class GuidanceUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "guidance").mkdir()
        (self.root / "tests").mkdir()
        (self.root / "tests/test_example.py").write_text(
            'raise RuntimeError("related tests must not be imported")\n'
            'def test_result():\n    raise AssertionError("must not be run")\n',
            encoding="utf-8",
        )
        (self.root / guidance.SOURCE).write_text(HEADER + ENTRY, encoding="utf-8")

    def records(self, entry: str = ENTRY):
        return guidance.parse(HEADER + entry)

    def invoke(self, command: str) -> int:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return guidance.main([command, "--root", str(self.root)])

    def test_exact_shape(self) -> None:
        record = self.records()[0]
        self.assertEqual(record.id, "prefer-one-result")
        self.assertEqual(record.related_tests[0].node, "tests/test_example.py::test_result")

    def test_no_tests_is_valid(self) -> None:
        records = self.records(EMPTY_TEST_ENTRY)
        guidance.check_references(records, self.root)
        self.assertIn("No related test is recorded.", guidance.render(records))

    def test_empty_register_is_valid(self) -> None:
        self.assertEqual(guidance.parse(HEADER + "heuristics = []\n"), ())

    def test_malformed_toml(self) -> None:
        with self.assertRaises(guidance.GuidanceError):
            guidance.parse("not = [valid")

    def test_wrong_schema(self) -> None:
        with self.assertRaises(guidance.GuidanceError):
            guidance.parse((HEADER + ENTRY).replace("nonbinding-guidance", "contract"))

    def test_no_contractual_classification_or_authority_field(self) -> None:
        for field in ("authority", "classification", "contract_effect", "binding"):
            with self.subTest(field=field), self.assertRaises(guidance.GuidanceError):
                guidance.parse(HEADER + f'{field} = "external_contract"\n' + ENTRY)

    def test_no_per_record_contractual_status(self) -> None:
        text = EMPTY_TEST_ENTRY.replace('related_tests = []', 'related_tests = []\nstatus = "binding"')
        with self.assertRaises(guidance.GuidanceError):
            self.records(text)

    def test_missing_field(self) -> None:
        with self.assertRaises(guidance.GuidanceError):
            self.records(ENTRY.replace('scope = "Comparable presentations."\n', ""))

    def test_blank_text(self) -> None:
        with self.assertRaises(guidance.GuidanceError):
            self.records(ENTRY.replace('Prefer sharing one result.', '  '))

    def test_related_tests_must_be_a_list(self) -> None:
        with self.assertRaises(guidance.GuidanceError):
            self.records(EMPTY_TEST_ENTRY.replace('related_tests = []', 'related_tests = false'))

    def test_missing_relation_note(self) -> None:
        with self.assertRaises(guidance.GuidanceError):
            self.records(ENTRY.replace('note = "A narrow example, not a general guarantee."\n', ""))

    def test_duplicate_id(self) -> None:
        with self.assertRaises(guidance.GuidanceError):
            self.records(ENTRY + ENTRY)

    def test_duplicate_link_within_one_heuristic(self) -> None:
        link = ENTRY[ENTRY.index("[[heuristics.related_tests]]"):]
        with self.assertRaises(guidance.GuidanceError):
            self.records(ENTRY + link)

    def test_one_test_may_relate_to_two_heuristics(self) -> None:
        records = self.records(ENTRY + ENTRY.replace("prefer-one-result", "prefer-exact-context"))
        guidance.check_references(records, self.root)

    def test_invalid_ids(self) -> None:
        for identity in ("../other", "binding/v1", "has space", "<script>"):
            with self.subTest(identity=identity), self.assertRaises(guidance.GuidanceError):
                self.records(ENTRY.replace("prefer-one-result", identity))

    def test_invalid_node_shapes(self) -> None:
        for node in (
            "/tmp/test_example.py::test_result",
            "../tests/test_example.py::test_result",
            "tests/../tests/test_example.py::test_result",
            "tests//test_example.py::test_result",
            "tests/test_example.py::test_result[one]",
            "tests/test_example.py::*",
            "tests/helper.py::test_result",
            "tests/test_example.py::helper",
            "https://example.invalid/test_example.py::test_result",
        ):
            with self.subTest(node=node), self.assertRaises(guidance.GuidanceError):
                self.records(ENTRY.replace("tests/test_example.py::test_result", node))

    def test_references_are_not_imported_or_executed(self) -> None:
        guidance.check_references(self.records(), self.root)

    def test_missing_file(self) -> None:
        (self.root / "tests/test_example.py").unlink()
        with self.assertRaises(guidance.GuidanceError):
            guidance.check_references(self.records(), self.root)

    def test_renamed_symbol(self) -> None:
        (self.root / "tests/test_example.py").write_text("def test_other(): pass\n")
        with self.assertRaises(guidance.GuidanceError):
            guidance.check_references(self.records(), self.root)

    def test_ambiguous_symbol(self) -> None:
        (self.root / "tests/test_example.py").write_text("def test_result(): pass\ndef test_result(): pass\n")
        with self.assertRaises(guidance.GuidanceError):
            guidance.check_references(self.records(), self.root)

    def test_async_function_and_class_methods(self) -> None:
        (self.root / "tests/test_example.py").write_text(
            "class TestExamples:\n    async def test_result(self): pass\n"
        )
        records = self.records(ENTRY.replace("::test_result", "::TestExamples::test_result"))
        guidance.check_references(records, self.root)

    def test_nonfunction_does_not_count(self) -> None:
        (self.root / "tests/test_example.py").write_text("class test_result: pass\n")
        with self.assertRaises(guidance.GuidanceError):
            guidance.check_references(self.records(), self.root)

    def test_nested_function_does_not_count_as_collected_test(self) -> None:
        (self.root / "tests/test_example.py").write_text("def wrapper():\n    def test_result(): pass\n")
        with self.assertRaises(guidance.GuidanceError):
            guidance.check_references(self.records(), self.root)

    def test_invalid_python_is_reported(self) -> None:
        (self.root / "tests/test_example.py").write_text("def test_result(:\n")
        with self.assertRaises(guidance.GuidanceError):
            guidance.check_references(self.records(), self.root)

    def test_out_of_root_symlink(self) -> None:
        outside = self.root.parent / (self.root.name + "_test_outside.py")
        outside.write_text("def test_result(): pass\n")
        self.addCleanup(outside.unlink)
        target = self.root / "tests/test_example.py"
        target.unlink()
        try:
            target.symlink_to(outside)
        except OSError as exc:
            self.skipTest(f"symlinks unavailable: {exc}")
        with self.assertRaises(guidance.GuidanceError):
            guidance.check_references(self.records(), self.root)

    def test_output_cannot_redirect_into_contracts(self) -> None:
        contract = self.root / "qualification/contracts/sentinel.md"
        contract.parent.mkdir(parents=True)
        contract.write_text("unchanged contract fixture")
        try:
            (self.root / guidance.OUTPUT).symlink_to(contract)
        except OSError as exc:
            self.skipTest(f"symlinks unavailable: {exc}")
        self.assertEqual(self.invoke("update"), 2)
        self.assertEqual(contract.read_text(), "unchanged contract fixture")

    def test_render_is_deterministic_and_nonbinding(self) -> None:
        records = self.records(ENTRY + ENTRY.replace("prefer-one-result", "another-preference"))
        self.assertEqual(guidance.render(records), guidance.render(tuple(reversed(records))))
        rendered = guidance.render(records)
        self.assertIn(guidance.NOTICE, rendered)
        self.assertEqual(rendered.count("(non-binding)"), 2)
        self.assertIn("../tests/test_example.py", rendered)
        self.assertNotIn("#L", rendered)
        self.assertNotIn("coverage percentage", rendered)

    def test_update_and_check(self) -> None:
        self.assertEqual(self.invoke("update"), 0)
        self.assertEqual(self.invoke("check"), 0)

    def test_missing_render_is_stale(self) -> None:
        self.assertEqual(self.invoke("check"), 2)
        self.assertFalse((self.root / guidance.OUTPUT).exists())

    def test_stale_render_is_not_silently_rewritten(self) -> None:
        output = self.root / guidance.OUTPUT
        output.write_text("stale\n")
        self.assertEqual(self.invoke("check"), 2)
        self.assertEqual(output.read_text(), "stale\n")

    def test_guidance_change_requires_render_update(self) -> None:
        self.assertEqual(self.invoke("update"), 0)
        source = self.root / guidance.SOURCE
        source.write_text(source.read_text().replace("Prefer sharing one result.", "Consider another design."))
        self.assertEqual(self.invoke("check"), 2)
        self.assertEqual(self.invoke("update"), 0)
        self.assertEqual(self.invoke("check"), 0)

    def test_update_writes_only_the_render(self) -> None:
        contract = self.root / "qualification/contracts/sentinel.json"
        contract.parent.mkdir(parents=True)
        contract.write_text('{"independent": true}\n')
        before = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(self.invoke("update"), 0)
        after = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(set(after) - set(before), {guidance.OUTPUT})
        self.assertTrue(all(after[path] == value for path, value in before.items()))


# Run these in the complete Riverhog checkout, not in the standalone proposal bundle.
class GuidanceRepositoryTests(unittest.TestCase):
    def test_current_register_references_and_render(self) -> None:
        self.assertEqual(guidance.main(["check", "--root", str(ROOT)]), 0)

    def test_contract_generation_does_not_consume_guidance(self) -> None:
        """Compare fresh projections, then deny guidance imports/reads in the second run."""
        source = r'''
import hashlib, json, os, sys
from pathlib import Path
root = Path(sys.argv[1]).resolve()
guarded = sys.argv[2] == "guarded"
seen = []
class GuidanceDependency(BaseException):
    pass
if guarded:
    def audit(event, args):
        forbidden = event == "import" and args[0] in {"guidance", "scripts.guidance"}
        if event in {"open", "os.listdir", "os.scandir"} and args:
            value = args[0]
            if isinstance(value, (str, bytes, os.PathLike)):
                # Do not perform filesystem calls within an audit hook.
                path = Path(os.path.abspath(os.fsdecode(value)))
                forbidden = forbidden or path.is_relative_to(root / "guidance")
                forbidden = forbidden or path == root / "scripts/guidance.py"
                forbidden = forbidden or (
                    path.parent == root / "scripts/__pycache__"
                    and path.name.startswith("guidance.")
                )
        if forbidden:
            seen.append((event, str(args)))
            raise GuidanceDependency("contract generation attempted to consume guidance")
    sys.addaudithook(audit)
sys.path.insert(0, str(root / "scripts"))
import contract_freeze
projection = contract_freeze.contract_projection()
if seen:
    raise AssertionError(seen)
payload = json.dumps(projection, sort_keys=True, separators=(",", ":")).encode()
print("PROJECTION_SHA256=" + hashlib.sha256(payload).hexdigest())
'''
        identities = []
        for mode in ("normal", "guarded"):
            completed = subprocess.run(
                [sys.executable, "-c", source, str(ROOT), mode],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=True,
                timeout=300,
            )
            identities.append(
                [line for line in completed.stdout.splitlines() if line.startswith("PROJECTION_SHA256=")]
            )
        self.assertEqual(len(identities[0]), 1)
        self.assertEqual(identities[0], identities[1])
