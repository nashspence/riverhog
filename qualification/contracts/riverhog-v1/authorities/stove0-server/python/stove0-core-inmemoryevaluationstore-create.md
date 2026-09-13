# stove0_core.InMemoryEvaluationStore.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryevaluationstore-create:20b9969e52 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c921d6f80a"></a>
| Field | Shape |
|---|---|
| <a id="s-78f8963d1a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-97528352e0"></a>`distribution` | "stove0-server" |
| <a id="s-0798657943"></a>`module` | "stove0_core" |
| <a id="s-ec76f1246f"></a>`name` | "create" |
| <a id="s-2e758274af"></a>`owner` | "stove0_core.InMemoryEvaluationStore" |
| <a id="s-d2b4de6da1"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryEvaluationStore](stove0-core-inmemoryevaluationstore.md)

## Governing policies

- <a id="pa-b30dfcad52"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryEvaluationStore.create`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f651a81a3c75fec5eb9c0f167da69727963ba014670e9a6bc8a3fb026bb0537b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'EvaluationRecord') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create",
  "owner": "stove0_core.InMemoryEvaluationStore",
  "unit": "member"
}
```
