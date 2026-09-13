# riverhog_storage_adapter_protocol.WriteStartRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writestartrequest:5e161bc919 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1d2597b3cb"></a>
| Field | Shape |
|---|---|
| <a id="s-28c0992d1e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6efb6d803a"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-48acf396cf"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-5ee1a04418"></a>`name` | "WriteStartRequest" |
| <a id="s-c445dfbb70"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteStartRequest.canonical_metadata](riverhog-storage-adapter-protocol-writestartrequest-canonical-metadata.md)
- [riverhog_storage_adapter_protocol.WriteStartRequest.canonical_path](riverhog-storage-adapter-protocol-writestartrequest-canonical-path.md)

## Governing policies

- <a id="pa-45ee3a3d90"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteStartRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c69cb9e4fb63e00d86a2c35cddbe283e561e54d5d1409813a644ac4f08bb5846 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4394746d6639230cabebf5d57a6e369f5e1401fe144a77641c7eb0aff144d2fa",
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: Annotated[int, Ge(ge=1)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteStartRequest",
  "unit": "export"
}
```
