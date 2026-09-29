from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest
from a_riverhog_macos_provenance_contract_lib import CONTRACT_BINDING
from a_riverhog_macos_provenance_observer import (
    ACLCapture,
    DarwinFileSystemInfo,
    MacOSBackend,
    MacOSProvenanceObserver,
)
from riverhog_provenance import validate_graph, validate_graph_fragment
from riverhog_provenance.native_capture import NativeCapturePolicy, NativeStat
from riverhog_provenance_contracts import ContractCatalog


class FakeMacOSNative:
    def __init__(self) -> None:
        self.values = {
            b"com.apple.ResourceFork": b"fork-data",
            b"com.apple.FinderInfo": b"F" * 32,
            b"com.apple.quarantine": b"0083;mock provenance",
            b"com.apple.metadata:kMDItemWhereFroms": b"mock metadata",
        }

    def file_attributes(self, fd: int):
        stat = os.fstat(fd)
        return {
            "birthtime_ns": stat.st_ctime_ns,
            "mtime_ns": stat.st_mtime_ns,
            "ctime_ns": stat.st_ctime_ns,
            "atime_ns": stat.st_atime_ns,
            "backup_time_ns": stat.st_mtime_ns - 1_000,
            "added_time_ns": stat.st_mtime_ns - 2_000,
            "flags": 0x20,
            "generation": 7,
            "document_id": 42,
            "file_id": stat.st_ino,
            "parent_id": 1,
            "finder_info": b"F" * 32,
            "total_size": stat.st_size + len(self.values[b"com.apple.ResourceFork"]),
            "allocation_size": 8192,
            "io_block_size": 4096,
            "data_length": stat.st_size,
            "data_allocation_size": 4096,
            "resource_fork_length": len(self.values[b"com.apple.ResourceFork"]),
            "resource_fork_allocation_size": 4096,
        }

    def filesystem_info(self, fd: int) -> DarwinFileSystemInfo:
        return DarwinFileSystemInfo(
            fs_type="apfs",
            mount_point=b"/",
            mounted_from=b"/dev/disk3s1",
            fsid=(1, 2),
            flags=0,
            subtype=0,
            io_size=4096,
            block_size=4096,
        )

    def volume_attributes(self, mount_point: bytes):
        return {
            "uuid": "12345678-1234-5678-1234-567812345678",
            "capabilities": [0x300, 0x6400, 0, 0],
            "valid_capabilities": [0x300, 0x6400, 0, 0],
        }

    def list_xattrs(self, fd: int):
        return list(self.values)

    def xattr_size(self, fd: int, name: bytes) -> int:
        return len(self.values[name])

    def get_xattr(self, fd: int, name: bytes, maximum: int):
        value = self.values[name]
        return len(value), value if len(value) <= maximum else None

    def digest_resource_fork(self, fd: int, name: bytes, *, chunk_bytes: int):
        value = self.values[name]
        return len(value), hashlib.sha256(value).hexdigest()

    def get_acl(self, fd: int):
        return ACLCapture(raw=b"darwin-acl-external", text="!#acl 1\n")

    def sysctl_text(self, name: str):
        return {
            "kern.osproductversion": "26.6",
            "kern.osversion": "25G84",
            "hw.model": "Mac16,1",
        }.get(name)


def _observe(path: Path, host_id: str, *, native=None, policy=None):
    observer = MacOSProvenanceObserver(native=native or FakeMacOSNative(), enforce_platform=False)
    return observer.observe(observer.source(path, host_id=host_id, policy=policy))


def _data(result):
    return result.observation["profiles"][0]["data"]


def test_mocked_macos_observation_contract(tmp_path: Path, urn_factory) -> None:
    payload = tmp_path / "photo.jpg"
    content = b"opaque image bytes; never parsed"
    payload.write_bytes(content)
    result = _observe(payload, urn_factory())
    graph = result.graph_fragment()
    validate_graph_fragment(graph)
    assert (
        result.observation["content"]["digests"][0]["value"] == hashlib.sha256(content).hexdigest()
    )
    assert result.observation["content"]["size_bytes"] == str(len(content))
    assert graph["states"][0]["extent"] == {"kind": "whole_object"}
    assert graph["locator_bindings"][0]["target"]["object_id"] == result.state_id
    data = _data(result)
    kinds = {row["kind"] for row in data["native_metadata"]}
    assert {
        "resource_fork",
        "finder_info",
        "security_label",
        "acl",
        "file_flag",
        "sparse_map",
        "native_stat_field",
    }.issubset(kinds)
    assert data["environment"]["operating_system"]["version"] == "26.6"
    assert data["environment"]["filesystem"]["type"] == "apfs"
    assert data["environment"]["filesystem"]["case_sensitive"] is True
    assert graph["activities"][0]["outcome"] == "success"
    assert result.observation["profiles"][0]["profile"]["schema_id"].endswith(
        "/macos-native-capture.json"
    )


def test_mocked_large_resource_fork_is_digest_only(tmp_path: Path, urn_factory) -> None:
    native = FakeMacOSNative()
    native.values[b"com.apple.ResourceFork"] = b"R" * 128
    payload = tmp_path / "movie.mov"
    payload.write_bytes(b"payload")
    result = _observe(
        payload,
        urn_factory(),
        native=native,
        policy=NativeCapturePolicy(inline_native_value_bytes=16, maximum_native_value_bytes=1024),
    )
    fork = next(row for row in _data(result)["native_metadata"] if row["kind"] == "resource_fork")
    assert fork["capture_status"] == "digest_only"
    assert fork["value"]["byte_length"] == "128"
    validate_graph_fragment(result.graph_fragment())


def test_macos_volume_uuid_authority_is_host_independent(urn_factory) -> None:
    stat = NativeStat(
        device=9,
        inode=42,
        mode=0o100644,
        nlink=1,
        uid=501,
        gid=20,
        size=7,
        atime_ns=1,
        mtime_ns=2,
        ctime_ns=3,
    )
    fs_info = DarwinFileSystemInfo(
        fs_type="apfs",
        mount_point=b"/Volumes/Archive",
        mounted_from=b"/dev/disk4s1",
        fsid=(11, 22),
        flags=0,
        subtype=0,
        io_size=4096,
        block_size=4096,
    )
    volume_attrs = {"uuid": "12345678-1234-5678-1234-567812345678"}
    first = MacOSBackend._volume_authority(urn_factory(), stat, fs_info, volume_attrs)
    second = MacOSBackend._volume_authority(urn_factory(), stat, fs_info, volume_attrs)
    assert first == second


def test_macos_local_volume_authority_remains_host_scoped(urn_factory) -> None:
    stat = NativeStat(
        device=9,
        inode=42,
        mode=0o100644,
        nlink=1,
        uid=501,
        gid=20,
        size=7,
        atime_ns=1,
        mtime_ns=2,
        ctime_ns=3,
    )
    fs_info = DarwinFileSystemInfo(
        fs_type="apfs",
        mount_point=b"/Volumes/Archive",
        mounted_from=b"/dev/disk4s1",
        fsid=(11, 22),
        flags=0,
        subtype=0,
        io_size=4096,
        block_size=4096,
    )
    first = MacOSBackend._volume_authority(urn_factory(), stat, fs_info, {})
    second = MacOSBackend._volume_authority(urn_factory(), stat, fs_info, {})
    assert first != second


class _VolumeAttributesFailureMacOSNative(FakeMacOSNative):
    def volume_attributes(self, mount_point: bytes):
        raise OSError(5, "mock I/O failure")


class _EmptyVolumeTextMacOSNative(FakeMacOSNative):
    def filesystem_info(self, fd: int) -> DarwinFileSystemInfo:
        info = super().filesystem_info(fd)
        return DarwinFileSystemInfo(
            fs_type="",
            mount_point=info.mount_point,
            mounted_from=b"",
            fsid=info.fsid,
            flags=info.flags,
            subtype=info.subtype,
            io_size=info.io_size,
            block_size=info.block_size,
        )


def test_macos_omits_unavailable_empty_volume_observations(tmp_path: Path, urn_factory) -> None:
    payload = tmp_path / "empty-volume-fields.dat"
    payload.write_bytes(b"payload")
    result = _observe(payload, urn_factory(), native=_EmptyVolumeTextMacOSNative())
    data = _data(result)
    filesystem = data["environment"]["filesystem"]
    assert filesystem["type"] == "unknown"
    assert all(item["value"] for item in filesystem["volume_identifiers"])
    assert {item["scheme"] for item in filesystem["volume_identifiers"]} == {
        "darwin-fsid",
        "volume-uuid",
    }
    volume_context = next(
        item
        for item in result.graph_fragment()["extensions"]
        if item["property"].endswith("/macos-volume-context")
    )
    assert volume_context["value"]["value"]["data"]["filesystem_type"] == "unknown"
    validate_graph_fragment(result.graph_fragment())


def test_macos_volume_attribute_failure_retains_fstatfs_context(
    tmp_path: Path, urn_factory
) -> None:
    payload = tmp_path / "volume-context.dat"
    payload.write_bytes(b"payload")
    result = _observe(payload, urn_factory(), native=_VolumeAttributesFailureMacOSNative())
    data = _data(result)
    assert data["environment"]["filesystem"]["type"] == "apfs"
    assert data["coverage"]["native_identifiers"] == "partial"
    assert result.graph_fragment()["activities"][0]["outcome"] == "partial"
    assert any(item["code"] == "volume_attributes_unavailable" for item in data["diagnostics"])
    validate_graph_fragment(result.graph_fragment())


def test_macos_volume_context_is_a_pinned_validated_assertion(tmp_path: Path, urn_factory) -> None:
    payload = tmp_path / "volume.dat"
    payload.write_bytes(b"volume")
    graph = _observe(payload, urn_factory()).graph_fragment()
    volume = next(
        row for row in graph["extensions"] if row["property"].endswith("/macos-volume-context")
    )
    assert volume["value"]["value"]["profile"]["contract_sha256"] == (
        CONTRACT_BINDING.contract_sha256
    )
    volume["value"]["value"]["data"]["filesystem_type"] = ""
    with pytest.raises(ValueError, match="schema validation"):
        validate_graph(graph, catalog=ContractCatalog((CONTRACT_BINDING,)))
