# stove0_core.PlanningPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-planningport:264620f2e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-717a06d156"></a>
- <a id="s-2b0118f44a"></a>`distribution`: `stove0-server`
- <a id="s-ec1b4167bb"></a>`module`: `stove0_core`
- <a id="s-63dca16d03"></a>`name`: `PlanningPort`
- <a id="s-3b2fcc5004"></a>`unit`: `export`

### Declared structure

- <a id="s-5a657f36d6"></a>`kind`: `"class"`
- <a id="s-202506cccd"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [stove0_core.PlanningPort.observation_requests](stove0-core-planningport-observation-requests.md)
- [stove0_core.PlanningPort.operation_contract](stove0-core-planningport-operation-contract.md)
- [stove0_core.PlanningPort.target_input_selection](stove0-core-planningport-target-input-selection.md)
- [stove0_core.PlanningPort.target_preflight_request](stove0-core-planningport-target-preflight-request.md)
- [stove0_core.PlanningPort.workflow_plan](stove0-core-planningport-workflow-plan.md)

## Governing policies

- <a id="pa-fde993d2c4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PlanningPort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0411d49640447b32a40ef6da420ff23f99d9211024073c0c1ef76b8a92d5ec62 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "PlanningPort",
  "unit": "export"
}
```
