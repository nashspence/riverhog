# riverhog_storage_adapter_protocol.StorageAdapterErrorBody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-0700064632:86305ef29f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94ec0395e6"></a>
| Field | Shape |
|---|---|
| <a id="s-a899e753de"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-faf3548d4e"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-434e48163d"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-043b5a4df8"></a>`name` | "StorageAdapterErrorBody" |
| <a id="s-edfc672dd1"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8afd00627f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterErrorBody`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e73d7ef34e552c7c3f57a9ab030577eb5e8cfb0e005ca089ceb0886d9f077fb0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9c6d2bb482f12292a4cb9eca4a79d1e6c953362f34ba69a7f5009bf0d394616d",
    "signature": "\"(*, code: Literal['unauthorized', 'invalid_request', 'not_found', 'method_not_allowed', 'length_required', 'request_too_large', 'insufficient_storage', 'identity_conflict', 'traversal_invalidated', 'invalid_path', 'invalid_range', 'read_not_ready', 'read_expired', 'integrity_failure', 'provider_unavailable', 'internal_failure'], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2000)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "StorageAdapterErrorBody",
  "unit": "export"
}
```
