# stove0_core.scheduler_role

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-scheduler-role:6bdf99025e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f944398b3"></a>
| Field | Shape |
|---|---|
| <a id="s-bddc7114c7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b025422a6f"></a>`distribution` | "stove0-server" |
| <a id="s-57f0c1f417"></a>`module` | "stove0_core" |
| <a id="s-a200110cc3"></a>`name` | "scheduler_role" |
| <a id="s-a1578e57f8"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6974655056"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.scheduler_role`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 595cc80b57452d7ad6319f33b361223ce2b0c2f81d7847aaae2ff92f596fea3b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'SchedulerRole'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "scheduler_role",
  "unit": "export"
}
```
