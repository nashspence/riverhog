# stove0_core.Stove0WorkService.seal_target_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-seal-target-plan:32dbe29491 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ddc0a99294"></a>
- <a id="s-3d4b6ebdde"></a>`distribution`: `stove0-server`
- <a id="s-5ff1ba9e2d"></a>`module`: `stove0_core`
- <a id="s-e43d18b2cf"></a>`name`: `seal_target_plan`
- <a id="s-339ca07f87"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-e1735697fb"></a>`unit`: `member`

### Declared structure

- <a id="s-7920dd0aa1"></a>`kind`: `"method"`
- <a id="s-19f2d03e2d"></a>`signature`: `"\"(self, work_id: 'str', *, target: 'TargetContract', plan: 'TargetPlan', expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-babc354869"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.seal_target_plan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6186a0f4c6b5fccf7619f702b1972479283f2b6112a7748b162c8f0f5fcd413 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, target: 'TargetContract', plan: 'TargetPlan', expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_target_plan",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```
