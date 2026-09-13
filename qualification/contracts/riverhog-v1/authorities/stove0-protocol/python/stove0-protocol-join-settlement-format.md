# stove0_protocol.JOIN_SETTLEMENT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-join-settlement-format:475d78cf1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ebc78acaa3"></a>
| Field | Shape |
|---|---|
| <a id="s-3f7e077a1d"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-92c8f561fb"></a>`distribution` | "stove0-protocol" |
| <a id="s-b749c84dcb"></a>`module` | "stove0_protocol" |
| <a id="s-47be06af62"></a>`name` | "JOIN_SETTLEMENT_FORMAT" |
| <a id="s-5307e22423"></a>`unit` | "export" |

## Governing policies

- <a id="pa-59455ef55d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JOIN_SETTLEMENT_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cee25cb64ddb13e14e0eb8b8d2f4633d6fbe97f7cd4928c7c77cdfaf25b238c3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-join-settlement/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JOIN_SETTLEMENT_FORMAT",
  "unit": "export"
}
```
