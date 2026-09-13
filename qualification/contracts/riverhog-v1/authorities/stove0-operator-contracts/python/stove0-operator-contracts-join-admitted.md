# stove0_operator_contracts.JOIN_ADMITTED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-join-admitted:92be19df9f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3381b1a00d"></a>
| Field | Shape |
|---|---|
| <a id="s-5e3cc232c5"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-59dac2badc"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-4bb0d0d629"></a>`module` | "stove0_operator_contracts" |
| <a id="s-5db8c64df4"></a>`name` | "JOIN_ADMITTED" |
| <a id="s-1b61b3514f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a205e01250"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JOIN_ADMITTED`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62067e72ab4001b10fe7a7df0b295b448b2562c9b4606cb7e64ecab8f482751b -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "io.riverhog.stove0.join.admitted"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "JOIN_ADMITTED",
  "unit": "export"
}
```
