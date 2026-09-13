# stove0_core.InMemoryWorkStore.ensure_target_production_receiving

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-ensure-targ-4e6ae911f2:460d834e2a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc2b6fd068"></a>
| Field | Shape |
|---|---|
| <a id="s-35b3948c8e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0eeef24673"></a>`distribution` | "stove0-server" |
| <a id="s-e4bbaadd26"></a>`module` | "stove0_core" |
| <a id="s-5f3404086f"></a>`name` | "ensure_target_production_receiving" |
| <a id="s-163dc6ebc9"></a>`owner` | "stove0_core.InMemoryWorkStore" |
| <a id="s-5176f12bcc"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-28bdc92e7c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.ensure_target_production_receiving`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76e92e37cea7e9bf0115a46d204f6c5d14c2eaf2190be80669ad12882ff70e42 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ensure_target_production_receiving",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
