"""Run reference tests in an expanded source checkout; never apply changes implicitly."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import types


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--adapters', action='store_true')
    parser.add_argument('--fixture-codec', action='store_true',
                        help='Diagnostic JSON I/O ONLY; never native JCS qualification.')
    args = parser.parse_args()
    root = args.repo.resolve(strict=True)
    if not (root / 'scripts/validation_evidence.py').is_file():
        parser.error('prepare the expanded source checkout using validation-evidence.patch first')
    if args.fixture_codec and not args.adapters:
        parser.error('--fixture-codec requires --adapters and is diagnostic only')
    os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    sys.path.insert(0, str(root))
    tests = ['test_validation_evidence.py']
    if args.adapters:
        tests += ['test_validation_timing_policy.py', 'test_transfer_profile_script.py']
    if args.fixture_codec:
        print('DIAGNOSTIC JSON fixture: not native RFC8785 identity or locked qualification.', flush=True)
        codec = types.ModuleType('riverhog_canonical_json')
        def encoded(value):
            return json.dumps(value, ensure_ascii=False, allow_nan=False,
                              sort_keys=True, separators=(',', ':')).encode('utf-8')
        def decoded(data):
            value = json.loads(data)
            if encoded(value) != data:
                raise ValueError('fixture serialization mismatch; not a native JCS check')
            return value
        codec.canonical_json_bytes = encoded
        codec.require_canonical_json = decoded
        codec.canonical_json_sha256 = lambda value: hashlib.sha256(encoded(value)).hexdigest()
        sys.modules['riverhog_canonical_json'] = codec
    import pytest
    return pytest.main(['--confcutdir='+str(root/'tests/unit'), '-q', '-W', 'error',
                        '-o', 'addopts=', *[str(root/'tests/unit'/name) for name in tests]])


if __name__ == '__main__':
    raise SystemExit(main())
