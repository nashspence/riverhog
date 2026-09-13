# stove0_operator_contracts.SchedulerRole

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerrole:71877bb925 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45c9463ea6"></a>
| Field | Shape |
|---|---|
| <a id="s-c4c07e7203"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-15cbdad3a9"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-54b401c302"></a>`module` | "stove0_operator_contracts" |
| <a id="s-4c7b78811d"></a>`name` | "SchedulerRole" |
| <a id="s-4d5960a380"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b38f7268af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerRole`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9c81007bc765213c407d2e1d46d4754e28b0a2ce713adb40c75117291cf05ad -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerRole",
  "unit": "export"
}
```
