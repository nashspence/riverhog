# stove0_core.WorkStore.ensure_target_production_receiving

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-ensure-target-produ-60b0ca2462:a4a7fba952 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d28bb583e1"></a>
- <a id="s-7fe603282d"></a>`distribution`: `stove0-server`
- <a id="s-f805369a06"></a>`module`: `stove0_core`
- <a id="s-ebff77cba7"></a>`name`: `ensure_target_production_receiving`
- <a id="s-4c8e289ac3"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-2c93e5ea31"></a>`unit`: `member`

### Declared structure

- <a id="s-251ae7dcc1"></a>`kind`: `"method"`
- <a id="s-7db2a3c992"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-5e8537c13e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.ensure_target_production_receiving`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ef00aca3861dd60d4c192777934c520e6408fcec8612d54c1213843969a21fb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ensure_target_production_receiving",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
