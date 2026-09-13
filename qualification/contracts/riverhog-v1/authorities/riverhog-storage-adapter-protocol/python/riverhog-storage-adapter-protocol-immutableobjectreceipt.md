# riverhog_storage_adapter_protocol.ImmutableObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-immutab-95787d4876:19cc2b5cdd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0cf57aa068"></a>
| Field | Shape |
|---|---|
| <a id="s-26671d3200"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3d3bf3a29e"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-f84e9690fc"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-d6a3334737"></a>`name` | "ImmutableObjectReceipt" |
| <a id="s-bb7a0b9c3d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ImmutableObjectReceipt.canonical_completed_at](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-completed-at.md)
- [riverhog_storage_adapter_protocol.ImmutableObjectReceipt.canonical_path](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-path.md)
- [riverhog_storage_adapter_protocol.ImmutableObjectReceipt.canonical_metadata](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-metadata.md)

## Governing policies

- <a id="pa-8743403251"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ImmutableObjectReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c6edf2db0331a32a2b835a52ea869fe33e592d4d4b3b74f454d55e99f6c37ac -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4246d342ca4bea1f0e76e6b31ee8d998513e8d604a1b2ef888c96c7a0776196d",
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ImmutableObjectReceipt",
  "unit": "export"
}
```
