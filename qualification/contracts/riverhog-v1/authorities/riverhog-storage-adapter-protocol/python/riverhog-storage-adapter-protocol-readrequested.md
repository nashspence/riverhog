# riverhog_storage_adapter_protocol.ReadRequested

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readrequested:af180e827a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a7acfd5f1"></a>
| Field | Shape |
|---|---|
| <a id="s-177517ec91"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-76a87a25ec"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-25cda13643"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-94065ea87d"></a>`name` | "ReadRequested" |
| <a id="s-97decf1a13"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ReadRequested.canonical_estimated_ready_at](riverhog-storage-adapter-protocol-readrequested-canonical-estimated-ready-at.md)

## Governing policies

- <a id="pa-9df9317e04"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadRequested`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c52dec520e3e8d3e1fe9f3ea1f6a4204be35f52e5af497560a22317321c2036 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d31c1bb220eaafa6bbc0ed7ed7faaba5eaa8d145766373dbbc76f7f5f377c2e7",
    "signature": "\"(*, state: Literal['requested'] = 'requested', estimated_ready_at: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=100)] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadRequested",
  "unit": "export"
}
```
