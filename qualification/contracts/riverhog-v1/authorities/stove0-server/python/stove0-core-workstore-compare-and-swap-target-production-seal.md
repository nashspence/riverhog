# stove0_core.WorkStore.compare_and_swap_target_production_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-compare-and-swap-ta-874d834140:76f543d691 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d78499e63"></a>
| Field | Shape |
|---|---|
| <a id="s-78db68fb89"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8ebbd08629"></a>`distribution` | "stove0-server" |
| <a id="s-20bb8e29e2"></a>`module` | "stove0_core" |
| <a id="s-ee6a669801"></a>`name` | "compare_and_swap_target_production_seal" |
| <a id="s-49aef95453"></a>`owner` | "stove0_core.WorkStore" |
| <a id="s-dc37908fc2"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-943166292e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.compare_and_swap_target_production_seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f14b119c048d6c12aa4e35a4b9e3dd9347195473dd9603fce46e6973bea2105f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_target_production_seal",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
