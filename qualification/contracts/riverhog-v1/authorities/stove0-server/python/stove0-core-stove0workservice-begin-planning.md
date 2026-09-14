# stove0_core.Stove0WorkService.begin_planning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-begin-planning:b043825156 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-088c35ac5c"></a>
- <a id="s-4ceff1db2f"></a>`distribution`: `stove0-server`
- <a id="s-a527a8ec09"></a>`module`: `stove0_core`
- <a id="s-3f941c24d7"></a>`name`: `begin_planning`
- <a id="s-abbb1bd643"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-81360e38c1"></a>`unit`: `member`

### Declared structure

- <a id="s-b2a28d466b"></a>`kind`: `"method"`
- <a id="s-35cfd3fbea"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-8f17efaded"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.begin_planning`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f044421094db93bc4c5b420e2566a4ee77a232c1709352d3f0fed5ec37cf46ac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_planning",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```
