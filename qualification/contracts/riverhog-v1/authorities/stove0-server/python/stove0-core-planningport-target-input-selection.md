# stove0_core.PlanningPort.target_input_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-planningport-target-input-selection:185eb44508 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1480a34ed5"></a>
- <a id="s-afb433ffce"></a>`distribution`: `stove0-server`
- <a id="s-c17abe8174"></a>`module`: `stove0_core`
- <a id="s-21d6351205"></a>`name`: `target_input_selection`
- <a id="s-e84fc95723"></a>`owner`: `stove0_core.PlanningPort`
- <a id="s-28abc883c3"></a>`unit`: `member`

### Declared structure

- <a id="s-7dad907e1e"></a>`kind`: `"method"`
- <a id="s-2f3e48c728"></a>`signature`: `"\"(self, plan: 'WorkflowPlan', selections: 'dict[str, ArtifactSelection]') -> 'ArtifactSelection'\""`

## Maintained corroboration

### Related interface records

- [PlanningPort](stove0-core-planningport.md)

## Governing policies

- <a id="pa-da7ea24495"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PlanningPort.target_input_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a8379642a961ac979f7b49e4d0b26a9f5ee222998961da15ee84de9c453680e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan: 'WorkflowPlan', selections: 'dict[str, ArtifactSelection]') -> 'ArtifactSelection'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_input_selection",
  "owner": "stove0_core.PlanningPort",
  "unit": "member"
}
```

</details>
