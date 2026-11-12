"""Run under an installed normal wheel; no repository imports or fault API."""
import json
import os
from pathlib import Path
import sys
import tempfile

from retained_resource import Borrow, Resource
from retained_resource.construction import Construction

assert not hasattr(Resource, "_test_fault")
assert ".reference" not in Resource.__module__
with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / "snapshot.sqlite3"
    resource = Resource("file")
    resource.acquire(os.fsencode(path) if os.name == "posix" else str(path), writable=True, exclusive=True)
    borrower = Borrow(resource)
    borrower.acquire()
    descriptor = borrower.fileno()
    os.write(descriptor, b"exact original")
    borrower.close()
    resource.sync()
    resource.close()
    other = os.open(path, os.O_RDONLY)
    if other != descriptor:
        os.dup2(other, descriptor)
        os.close(other)
    try:
        resource.close()
        assert os.read(descriptor, 14) == b"exact original"
        assert resource.metrics["close_calls"] == 1
    finally:
        os.close(descriptor)
    try:
        loser = Resource("file")
        loser.acquire(os.fsencode(path) if os.name == "posix" else str(path), writable=True, exclusive=True)
    except FileExistsError:
        loser.close()
    else:
        raise AssertionError("exclusive creation replaced the original")
    assert path.read_bytes() == b"exact original"
print(json.dumps({"normal_installed_smoke": "passed", "platform": sys.platform,
                  "executable": sys.executable, "normal_has_no_test_api": True}))
