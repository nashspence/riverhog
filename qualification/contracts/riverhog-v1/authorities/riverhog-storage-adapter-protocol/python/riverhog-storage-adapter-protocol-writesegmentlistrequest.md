# riverhog_storage_adapter_protocol.WriteSegmentListRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-f8483c1c43:0df1f208f0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d3cbfbec1d"></a>
| Field | Shape |
|---|---|
| <a id="s-308b67eb91"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-aace5a99e0"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-930616bc27"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-13148a36ee"></a>`name` | "WriteSegmentListRequest" |
| <a id="s-e990b6d6c0"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3cb31a2dab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentListRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1f5d16415e6da18f4f990dd0631efc1aee0695ab96647c646d7331c705b5727 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c655ec3f5640ade7ac2d3b60a3b9bb3de1ecaf39de3d4bcd731afb029a65684f",
    "signature": "'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, after_number: Annotated[int, Ge(ge=0)] = 0, traversal_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, maximum_items: Annotated[int, Ge(ge=1), Le(le=128)] = 128) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentListRequest",
  "unit": "export"
}
```
