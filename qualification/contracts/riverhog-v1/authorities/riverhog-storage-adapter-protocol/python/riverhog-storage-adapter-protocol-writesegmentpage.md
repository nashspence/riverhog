# riverhog_storage_adapter_protocol.WriteSegmentPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writesegmentpage:b29423904d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4d3f473c69"></a>
| Field | Shape |
|---|---|
| <a id="s-0d72241444"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-887a4dc881"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-501165fd62"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-8d0c4f204c"></a>`name` | "WriteSegmentPage" |
| <a id="s-b8ef02192f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteSegmentPage.canonical_segments](riverhog-storage-adapter-protocol-writesegmentpage-canonical-segments.md)
- [riverhog_storage_adapter_protocol.WriteSegmentPage.validate_terminal](riverhog-storage-adapter-protocol-writesegmentpage-validate-terminal.md)

## Governing policies

- <a id="pa-86b3b91db0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1dfa72f8c2a67e70b90cb55fccd8a97dd3b6e5db7c56d498de0a8bd0be9de694 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d1d971aecf4fda3444d770576aa32bb6527302511e9401a9b027918ec8ae3b38",
    "signature": "'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, traversal_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)], segments: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.WriteSegmentReceipt, ...], MaxLen(max_length=128)] = (), next_after_number: Annotated[int | None, Ge(ge=1)] = None, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority | None = None) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentPage",
  "unit": "export"
}
```
