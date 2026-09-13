# riverhog_storage_adapter_protocol.WriteCompleteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-8bc312f21c:71bdb9f0ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50ef59c596"></a>
| Field | Shape |
|---|---|
| <a id="s-9571ea5ca6"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6f63e1d2ad"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-b543a074cb"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-779426bbd9"></a>`name` | "WriteCompleteRequest" |
| <a id="s-0ac38a7301"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteCompleteRequest.canonical_metadata](riverhog-storage-adapter-protocol-writecompleterequest-canonical-metadata.md)
- [riverhog_storage_adapter_protocol.WriteCompleteRequest.validate_bytes](riverhog-storage-adapter-protocol-writecompleterequest-validate-bytes.md)

## Governing policies

- <a id="pa-e6626ae0c9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompleteRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fae0a3db148549c231bd5f0dcc9e4486db86f610f3c4652176a5a4e3f120968 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3b6857dbea430d76de035e4b9a4d2e0306c6b78a8f4fd827d729da581b4e9f10",
    "signature": "\"(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority, expected_bytes: Annotated[int, Ge(ge=1)], expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteCompleteRequest",
  "unit": "export"
}
```
