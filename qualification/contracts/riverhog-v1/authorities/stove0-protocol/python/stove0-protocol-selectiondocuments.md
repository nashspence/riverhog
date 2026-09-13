# stove0_protocol.SelectionDocuments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-selectiondocuments:a6b3e6d5d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-751b6a260e"></a>
| Field | Shape |
|---|---|
| <a id="s-5f746280ba"></a>`contract` | type="types.GenericAlias"; additional keys=`kind` |
| <a id="s-e80a2fa78e"></a>`distribution` | "stove0-protocol" |
| <a id="s-413a91165e"></a>`module` | "stove0_protocol" |
| <a id="s-250e280fb0"></a>`name` | "SelectionDocuments" |
| <a id="s-2693ed524f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b6496405f7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.SelectionDocuments`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a80dea23d5cb956d528e77f840131180f7bf26cd7802b52632be0af65585f95 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "types.GenericAlias"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "SelectionDocuments",
  "unit": "export"
}
```
