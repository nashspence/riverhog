# riverhog_storage_adapter_protocol.ObjectHeadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectheadrequest:c864ad29b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eef88604e6"></a>
| Field | Shape |
|---|---|
| <a id="s-8e639c2c97"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d83845eb3f"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-7785b73456"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-c7d8f054a9"></a>`name` | "ObjectHeadRequest" |
| <a id="s-399b5cf29e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c5e98fc1b4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectHeadRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2cf194ae933032077ddb3c0a9dd4fd7401d6624eb9c5ce62146e89d7487dde1c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1e0283dd234d9ca0e0f4a026cfc1d8102dd40e8a31f86eb9e99e8613976200e5",
    "signature": "\"(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectHeadRequest",
  "unit": "export"
}
```
