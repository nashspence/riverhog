# stove0_operator_contracts.SchedulerRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerrun:7c5d6153ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9052785ed5"></a>
| Field | Shape |
|---|---|
| <a id="s-5e19a19241"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e6d0614c12"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-3af0525ee1"></a>`module` | "stove0_operator_contracts" |
| <a id="s-a21009d9bc"></a>`name` | "SchedulerRun" |
| <a id="s-7d4de8abc1"></a>`unit` | "export" |

## Governing policies

- <a id="pa-324752f5b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerRun`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd91224d04b2cb958ccc56c01573930072a5e8e6096bd6a4062a649cf5e5f702 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "fee1ffcab5fa9cfca8ccd9dbcdb1291b4545b6a161c886b5f6b8402be1501bb2",
    "signature": "'(*, pruning: stove0_operator_contracts.SchedulerPruning | None, admission: stove0_operator_contracts.AdmissionRun | None = None, work: stove0_operator_contracts.SchedulerWorkBatch) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerRun",
  "unit": "export"
}
```
