# riverhog_protocol.CollectionUploadUnitState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitstate:24c2712190 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75d7c130a9"></a>
| Field | Shape |
|---|---|
| <a id="s-1c3cc69080"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-3e7e0bee76"></a>`distribution` | "riverhog-protocol" |
| <a id="s-07d8cc4181"></a>`module` | "riverhog_protocol" |
| <a id="s-29734fd814"></a>`name` | "CollectionUploadUnitState" |
| <a id="s-f85c9cc1d3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-13b3b5a391"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitState`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c83a460ffdb753c79d642740481db9e54efd989ede1551006da454bb0c605df -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitState",
  "unit": "export"
}
```
