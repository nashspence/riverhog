from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
import riverhog_api.deps as deps
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_protocol.errors import ServiceUnavailable
from riverhog_storage_adapter_protocol import (
    AdapterDescriptor,
    BeginWriteOperation,
    CleanupInventoryProgress,
    CleanupInventoryRequest,
    CleanupInventoryStepRequest,
    CleanupInvocation,
    StorageAdapterPort,
    WriteCallIdentity,
    cleanup_step_sha256,
)

from tests.unit.test_archive_write_execution import _executor, _filesystem, _invocation, _start


class _Adapter:
    def descriptor(self) -> AdapterDescriptor:
        return AdapterDescriptor(
            storage_incarnation_id="00000000-0000-4000-8000-000000000001",
            implementation_id="fixture.storage/v1",
            implementation_version="1.0.0",
            read_mode="immediate",
            minimum_nonfinal_segment_bytes=1,
            maximum_segment_bytes=1024,
            maximum_segment_count=10_000,
        )


def test_composition_binds_every_capability_to_each_configured_archive_store(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    base = RuntimeConfig.for_testing()
    primary = base.archive_store("archive")
    secondary = replace(primary, name="secondary", base_url="http://127.0.0.1/secondary")
    config = replace(
        base,
        archive_stores={"archive": primary, "secondary": secondary},
        archive_read_order=("secondary", "archive"),
    )
    adapters = {
        name: cast(StorageAdapterPort, cast(Any, _Adapter())) for name in config.archive_stores
    }

    registry = deps._archive_store_registry(
        config,
        adapters=adapters,  # type: ignore[arg-type]
        download_allowance=object(),  # type: ignore[arg-type]
    )

    assert registry.names == tuple(config.archive_stores)
    for name in registry.names:
        binding = registry.require(name)
        expected = adapters[name]
        validated = binding.store._adapter  # type: ignore[attr-defined]
        assert validated._adapter is expected  # type: ignore[attr-defined]
        assert binding.resumable_objects._adapter is validated  # type: ignore[attr-defined]
        assert binding.require_resumable_writes() is binding.resumable_objects
        assert binding.require_object_cleanup()._adapter is validated  # type: ignore[attr-defined]
        assert binding.require_immutable_writes() is binding.immutable_objects
        assert binding.immutable_objects._adapter is validated  # type: ignore[attr-defined]
        assert binding.object_ranges._adapter is validated  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("capability", "method"),
    (
        ("resumable_writes", "require_resumable_writes"),
        ("object_cleanup", "require_object_cleanup"),
    ),
)
def test_missing_execution_capabilities_do_not_fall_back_to_unbounded_archive_methods(
    capability: str, method: str
) -> None:
    binding = deps._archive_store_binding(
        RuntimeConfig.for_testing(), "archive", cast(Any, _Adapter()), cast(Any, object())
    )
    with pytest.raises(ServiceUnavailable, match="port"):
        getattr(replace(binding, **{capability: None}), method)()


@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_composed_original_write_and_bounded_inventory_use_the_selected_adapter(
    tmp_path: Path, mode: str
) -> None:
    with _filesystem(tmp_path / "provider") as adapter, _executor(adapter, mode) as client:
        binding = deps._archive_store_binding(
            RuntimeConfig.for_testing(), "archive", client, cast(Any, object())
        )
        incarnation = binding.incarnation_id
        opened = binding.require_resumable_writes().execute_write(
            _invocation(BeginWriteOperation(request=_start()), incarnation)
        )
        assert opened.execution == "closed" and opened.outcome == "session"
        request = CleanupInventoryStepRequest(
            selected_terminal_sha256="a" * 64,
            inventory=CleanupInventoryRequest(
                object_prefix="archives",
                target_incarnation=incarnation,
                maximum_items=1,
                maximum_response_bytes=4096,
            ),
            maximum_provider_requests=1,
            maximum_control_bytes=8192,
            maximum_in_flight=1,
        )
        invocation = CleanupInvocation(
            call=WriteCallIdentity(
                call_id="b" * 64,
                exact_request_sha256=cleanup_step_sha256(request),
                target_incarnation=incarnation,
                dispatch_owner="c" * 64,
            ),
            request=request,
        )
        observed = binding.require_object_cleanup().execute_cleanup(invocation)
        assert observed.call == invocation.call
        assert observed.execution == "closed" and observed.outcome == "cleanup_inventory"
        assert isinstance(observed.result, CleanupInventoryProgress)
        page = observed.result.inventory
        assert page.target_incarnation == incarnation and page.scanned_items <= 1
        assert len(page.candidates) == 1
        candidate = page.candidates[0]
        assert candidate.kind == "open_write" and candidate.object_path == _start().object_path
