# stove0_core.Stove0Scheduler.advance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0scheduler-advance:27ab8854eb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef13251827"></a>
| Field | Shape |
|---|---|
| <a id="s-1a0c8e1723"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-abb8c7ca99"></a>`distribution` | "stove0-server" |
| <a id="s-50a0b51204"></a>`module` | "stove0_core" |
| <a id="s-bdffb7366c"></a>`name` | "advance" |
| <a id="s-8165679bda"></a>`owner` | "stove0_core.Stove0Scheduler" |
| <a id="s-5b39f27612"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0Scheduler](stove0-core-stove0scheduler.md)

## Governing policies

- <a id="pa-d8c7a640e0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0Scheduler.advance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b79a7b2eb88e2f21757fe0a613c8ff682f1090a45ec2721eda0efd1b56afec57 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, role: 'SchedulerRole' = 'combined', limit: 'int' = 25) -> 'dict[str, object]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "advance",
  "owner": "stove0_core.Stove0Scheduler",
  "unit": "member"
}
```
