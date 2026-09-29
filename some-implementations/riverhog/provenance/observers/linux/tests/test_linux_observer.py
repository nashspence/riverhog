from __future__ import annotations

import base64
import errno
import hashlib
import os
import sys
from pathlib import Path

import pytest
from a_riverhog_linux_provenance_observer import (
    FS_IOC_FSGETXATTR,
    FSXATTR_STRUCT_SIZE,
    LinuxBackend,
    LinuxProvenanceObserver,
    _portable_mount_field,
)
from riverhog_provenance import ObservationPolicy, ObservationRequest, validate_graph_fragment
from riverhog_provenance.native_capture import (
    NativeCapture,
    NativeCapturePolicy,
    NativeCaptureRequest,
    NativeStat,
    SymlinkRefusedError,
)
from riverhog_provenance_contracts import SOURCE_NAMING_VIEW_SCHEME

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux only")


def _observer() -> LinuxProvenanceObserver:
    return LinuxProvenanceObserver()


def _observe(path: Path | bytes, host_id: str, *, policy: NativeCapturePolicy | None = None):
    observer = _observer()
    return observer.observe(observer.source(path, host_id=host_id, policy=policy))


def _native_data(result):
    return result.observation["profiles"][0]["data"]


def test_live_linux_observation_is_canonical_and_measures_all_bytes(
    tmp_path: Path, urn_factory
) -> None:
    payload = tmp_path / "payload.bin"
    content = b"opaque primary bytes\x00\xff\n"
    payload.write_bytes(content)
    payload.chmod(0o644)
    try:
        os.setxattr(payload, b"user.riverhog-provenance-test", b"native-value")
    except OSError:
        pass

    result = _observe(payload, urn_factory())
    graph = result.graph_fragment()
    validate_graph_fragment(graph)
    assert graph["states"][0]["extent"] == {"kind": "whole_object"}
    assert result.observation["content"] == {
        "size_bytes": str(len(content)),
        "digests": [{"algorithm": "sha-256", "value": hashlib.sha256(content).hexdigest()}],
    }
    assert result.observation["consistency"]["level"] == "verified_unchanged"
    assert result.observation["profiles"][0]["profile"]["schema_id"].endswith(
        "/linux-native-capture.json"
    )
    assert _native_data(result)["access"]["posix_mode"] == "0644"
    assert graph["activities"][0]["outcome"] == "success"
    assert len(graph["locator_bindings"]) == 1
    assert graph["locator_bindings"][0]["target"] == {
        "object_id": result.state_id,
        "object_type": "state",
        "scope": "local",
    }
    assert "path" not in result.artifact
    assert "path" not in result.occurrence


def test_distinct_native_contexts_can_carry_one_explicit_source_naming_view(
    tmp_path: Path, urn_factory
) -> None:
    files = [tmp_path / "clip.mp4", tmp_path / "clip.xmp"]
    for path in files:
        path.write_bytes(b"opaque")
    observer = _observer()
    host_id = urn_factory()
    view_id = urn_factory()
    graphs = [
        observer.observe(observer.source(path, host_id=host_id, naming_view_id=view_id))
        .graph_fragment()
        for path in files
    ]
    contexts = [graph["contexts"][0] for graph in graphs]
    assert contexts[0]["id"] != contexts[1]["id"]
    for context in contexts:
        assert context["identifiers"] == [
            {
                "scheme": SOURCE_NAMING_VIEW_SCHEME,
                "value": {"kind": "text", "text": view_id},
                "scope": "global",
            }
        ]
    unscoped = observer.observe(observer.source(files[0], host_id=host_id))
    assert "identifiers" not in unscoped.graph_fragment()["contexts"][0]


def test_linux_raw_non_utf8_filename_is_preserved(tmp_path: Path, urn_factory) -> None:
    path = os.fsencode(tmp_path) + b"/name-\xff.bin"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, b"data")
    finally:
        os.close(fd)
    result = _observe(path, urn_factory())
    locator = result.graph_fragment()["locator_bindings"][0]["locator"]
    assert locator["name"]["encoding"] == "posix-bytes"
    assert base64.b64decode(locator["name"]["bytes"]["data"]) == os.path.abspath(path)


def test_symlink_final_component_is_refused(tmp_path: Path, urn_factory) -> None:
    target = tmp_path / "target"
    target.write_bytes(b"x")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(SymlinkRefusedError):
        _observe(link, urn_factory())


def test_regular_file_only(tmp_path: Path, urn_factory) -> None:
    from riverhog_provenance.native_capture import UnsupportedFileTypeError

    with pytest.raises(UnsupportedFileTypeError):
        _observe(tmp_path, urn_factory())


def test_native_policy_is_separate_from_canonical_observation_policy(
    tmp_path: Path, urn_factory
) -> None:
    payload = tmp_path / "policy.dat"
    payload.write_bytes(b"policy")
    observer = _observer()
    result = observer.observe(
        observer.source(
            payload, host_id=urn_factory(), policy=NativeCapturePolicy(capture_xattrs=False)
        ),
        ObservationRequest(),
    )
    assert _native_data(result)["coverage"]["extended_attributes"] == "not_requested"
    assert result.observation["coverage"][0]["status"] == "complete"


def test_fsgetxattr_ioctl_uses_exact_linux_fsxattr_size() -> None:
    assert FSXATTR_STRUCT_SIZE == 28
    assert (FS_IOC_FSGETXATTR >> 16) & 0x3FFF == FSXATTR_STRUCT_SIZE


class _ACLXattrOnlyNative:
    libacl = None

    @staticmethod
    def list_xattrs(fd: int):
        return [b"system.posix_acl_access"]

    @staticmethod
    def get_xattr(fd: int, name: bytes, maximum: int):
        value = b"opaque-acl-xattr"
        return len(value), value


def test_acl_xattr_counts_as_access_control_evidence_without_libacl(urn_factory) -> None:
    request = NativeCaptureRequest(host_id=urn_factory())
    backend = LinuxBackend(native=_ACLXattrOnlyNative(), enforce_platform=False)
    capture = NativeCapture()
    backend._capture_xattrs(0, request, capture)
    backend._capture_acl(0, request, capture)
    assert capture.coverage["access_control"] == "complete"
    assert capture.native_metadata[0]["kind"] == "acl"


def test_mountinfo_surrogate_bytes_get_portable_lossless_display() -> None:
    assert _portable_mount_field("source-\udcff") == "bytes:source-%FF"


class _LargeXattrNative:
    libacl = None

    @staticmethod
    def list_xattrs(fd: int):
        return [b"user.large"]

    @staticmethod
    def get_xattr(fd: int, name: bytes, maximum: int):
        return maximum + 1, None


def test_policy_not_retained_xattr_does_not_make_enumeration_partial(urn_factory) -> None:
    request = NativeCaptureRequest(
        host_id=urn_factory(),
        policy=NativeCapturePolicy(inline_native_value_bytes=4, maximum_native_value_bytes=8),
    )
    backend = LinuxBackend(native=_LargeXattrNative(), enforce_platform=False)
    capture = NativeCapture()
    backend._capture_xattrs(0, request, capture)
    assert capture.coverage["extended_attributes"] == "complete"
    assert capture.native_metadata[0]["capture_status"] == "not_retained"


def test_unexpected_getflags_failure_is_partial_not_complete(urn_factory, monkeypatch) -> None:
    import a_riverhog_linux_provenance_observer as linux_module

    backend = LinuxBackend(native=_ACLXattrOnlyNative(), enforce_platform=False)
    stat_snapshot = NativeStat(
        device=1,
        inode=2,
        mode=0o100644,
        nlink=1,
        uid=1000,
        gid=1000,
        size=3,
        atime_ns=1,
        mtime_ns=2,
        ctime_ns=3,
        extras={"statx_available": True, "statx_attributes": 0, "statx_attributes_mask": 0},
    )

    def denied(*args, **kwargs):
        raise OSError(errno.EACCES, os.strerror(errno.EACCES))

    monkeypatch.setattr(linux_module.fcntl, "ioctl", denied)
    capture = NativeCapture()
    backend._capture_file_flags(
        0, stat_snapshot, request=NativeCaptureRequest(host_id=urn_factory()), result=capture
    )
    assert capture.coverage["file_flags"] == "partial"
    assert capture.diagnostics[0]["severity"] == "error"


def test_native_metadata_budget_rejects_oversize_without_partial_assertion() -> None:
    from riverhog_provenance.native_capture import (
        BoundedNativeMetadata,
        NativeObservationError,
    )

    rows = BoundedNativeMetadata(maximum_bytes=32)
    rows.append({"kind": "a"})
    with pytest.raises(NativeObservationError, match="bounded capture budget"):
        rows.append({"value": "x" * 64})
    assert rows == [{"kind": "a"}]


def test_selected_provider_observes_native_file_with_exact_contract(
    tmp_path: Path, urn_factory
) -> None:
    from riverhog_provenance import resolve_provenance_observer

    payload = tmp_path / "selected.dat"
    payload.write_bytes(b"selected")
    selected = resolve_provenance_observer("a-riverhog-linux-provenance-observer")
    result = selected.observe_native_file(payload, host_id=urn_factory())
    detail = result.graph_fragment()["activities"][0]["details"][0]["data"]
    assert detail["provider"] == selected.name
    assert detail["contract"]["contract_sha256"] == selected.contract.contract_sha256
    assert result.catalog.validate_profile(
        result.observation["profiles"][0]["profile"],
        result.observation["profiles"][0]["data"],
    )


def test_second_content_measurement_uses_canonical_observation_policy(
    tmp_path: Path, urn_factory
) -> None:
    payload = tmp_path / "twice.bin"
    payload.write_bytes(b"measure twice")
    observer = _observer()
    result = observer.observe(
        observer.source(payload, host_id=urn_factory()),
        ObservationRequest(policy=ObservationPolicy(second_content_hash=True)),
    )
    assert result.observation["consistency"]["level"] == "verified_unchanged"
    assert result.graph_fragment()["activities"][0]["configuration"]["data"][
        "second_content_hash"
    ] is True
