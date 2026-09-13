# stove0_core.WorkStore.load_target_production_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load-target-production-seal:1dd7252b2c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6779167d9"></a>
| Field | Shape |
|---|---|
| <a id="s-d2864abcb6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-582a4c36ba"></a>`distribution` | "stove0-server" |
| <a id="s-ed489aa9a3"></a>`module` | "stove0_core" |
| <a id="s-ae6c1dd00c"></a>`name` | "load_target_production_seal" |
| <a id="s-27cacaa421"></a>`owner` | "stove0_core.WorkStore" |
| <a id="s-3c89b31bd9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-4c655749dc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load_target_production_seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16939176e6900c861613826d30580b5f6106de49f9325a41b4c3c9fc44a3adeb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_production_seal",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
