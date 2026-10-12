"""Navigation regression tests only; no product or model conformance claims."""
from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest

import read


class ReadingTests(unittest.TestCase):
    def test_bundle(self):
        result = read.check()
        self.assertEqual(result["families_routed"], len(read.family_entries()))
        self.assertFalse(result["runtime_conformance_tested"])

    def test_all_cards_fit_one_default_page(self):
        for row in read.packets():
            text = read.safe_path(row["card"]).read_text()
            self.assertLessEqual(len(text), read.DEFAULT_CHARS)

    def test_all_reads_page_without_loss(self):
        for row in read.packets():
            for source in row["reads"]:
                text = read.section(read.safe_path(source["path"]).read_text(), source["heading"])
                pieces, offset = [], 0
                while True:
                    part, following = read.page(text, offset, 317)
                    pieces.append(part)
                    if following is None:
                        break
                    offset = following
                self.assertEqual("".join(pieces), text)

    def test_heading_includes_children_not_sibling(self):
        text = '# x\n## A\nhello\n### child\nkeep\n## B\nno\n'
        self.assertEqual(read.section(text, '## A'), '## A\nhello\n### child\nkeep\n')

    def test_missing_heading(self):
        with self.assertRaises(ValueError):
            read.section('# a\n', '## Missing')

    def test_ambiguous_heading(self):
        with self.assertRaises(ValueError):
            read.section('## a\n## a\n', '## a')

    def test_path_escape(self):
        with self.assertRaises(ValueError):
            read.safe_path('../AGENTS.md')

    def test_output_limits(self):
        for offset, chars in [(-1, 10), (4, 10), (0, 0), (0, read.MAX_CHARS + 1)]:
            with self.assertRaises(ValueError):
                read.page('abc', offset, chars)

    def test_unicode_paging(self):
        text = 'é🧪字' * 100
        self.assertEqual(read.page(text, 2, 3)[0], text[2:5])

    def run_cli(self, *args):
        return subprocess.run([sys.executable, str(Path(read.__file__)), *args],
                              text=True, capture_output=True, check=False)

    def test_default_menu_not_cards(self):
        result = self.run_cli()
        self.assertEqual(result.returncode, 0)
        self.assertNotIn('## Outcome', result.stdout)
        self.assertLess(len(result.stdout), 4000)

    def test_single_family(self):
        result = self.run_cli('--family', 'G26')
        self.assertEqual(result.returncode, 0)
        self.assertIn('"id": "G26"', result.stdout)
        self.assertNotIn('"id": "G27"', result.stdout)

    def test_long_read_explicit_continuation(self):
        result = self.run_cli('P07', '--read', '1', '--chars', '300')
        self.assertEqual(result.returncode, 0)
        self.assertIn('NEXT:', result.stdout)
        self.assertLess(len(result.stdout), 1000)

    def test_bad_arguments(self):
        for args in [('P99',), ('--family', 'G99'), ('P00', '--read', '99'),
                     ('P00', '--family', 'G26'), ('--read', '1'),
                     ('--family', 'G26', '--read', '1')]:
            self.assertNotEqual(self.run_cli(*args).returncode, 0)


if __name__ == '__main__':
    unittest.main()
