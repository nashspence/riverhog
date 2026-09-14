# stove0_core.InMemoryWorkStore.scan_target_production_seals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-scan-target-8b6842759c:f296726be3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e5eb2ae55d"></a>
- <a id="s-7971359cf7"></a>`distribution`: `stove0-server`
- <a id="s-4fe24d3e03"></a>`module`: `stove0_core`
- <a id="s-8fc646e2dd"></a>`name`: `scan_target_production_seals`
- <a id="s-c3a00de9e3"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-f923ed949f"></a>`unit`: `member`

### Declared structure

- <a id="s-bf78840f8c"></a>`kind`: `"method"`
- <a id="s-32fc35078d"></a>`signature`: `"\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-6f2ac24290"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.scan_target_production_seals`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae2bca6752239f296732a614ed26bab1c4acbfcf1b622a7273adb6bf64483a2a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, state: 'TargetProductionSealState', limit: 'int') -> 'tuple[TargetProductionSealRecord, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "scan_target_production_seals",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
