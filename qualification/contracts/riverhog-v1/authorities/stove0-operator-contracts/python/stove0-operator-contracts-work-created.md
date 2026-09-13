# stove0_operator_contracts.WORK_CREATED

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-work-created:0b2ed569cc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42c4b1086d"></a>
| Field | Shape |
|---|---|
| <a id="s-fd87254e2b"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-4039f55dd1"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-d5ec4f0e12"></a>`module` | "stove0_operator_contracts" |
| <a id="s-77491ac80d"></a>`name` | "WORK_CREATED" |
| <a id="s-9eee2217a3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b73ce19a47"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WORK_CREATED`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec30aa60c4231f7e2bc0a25df4146ba07db3ee69d784e25717b5793c9f7a064f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "io.riverhog.stove0.work.created"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WORK_CREATED",
  "unit": "export"
}
```
