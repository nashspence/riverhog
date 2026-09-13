# riverhog_storage_adapter_protocol.CompletedObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-b3f1a3c579:cb4bdd68b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7080e1a03c"></a>
| Field | Shape |
|---|---|
| <a id="s-d91862f7fc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-bcf17ee6b6"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-50748a183c"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-b522b51ac3"></a>`name` | "CompletedObjectReceipt" |
| <a id="s-d148539706"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_completed_at](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-completed-at.md)
- [riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_path](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-path.md)
- [riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_metadata](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-metadata.md)

## Governing policies

- <a id="pa-6d4b7142ab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedObjectReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebeceb08f4008e59188822aa279099a87e5a61abd607c947a1b12da51a1d1842 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "209031565e0ef889aecef270878eea3fc57d0b15a42c37d7610d1dbf208def6a",
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=1)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "CompletedObjectReceipt",
  "unit": "export"
}
```
