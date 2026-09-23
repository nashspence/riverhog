# stove0_core.PlanningPort.target_preflight_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-planningport-target-preflight-request:ba991ff3c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f35caa74d9"></a>
- <a id="s-8e3c623741"></a>`distribution`: `stove0-server`
- <a id="s-f6bea64cce"></a>`module`: `stove0_core`
- <a id="s-a521ce6161"></a>`name`: `target_preflight_request`
- <a id="s-7c3887cb3c"></a>`owner`: `stove0_core.PlanningPort`
- <a id="s-6254f01d60"></a>`unit`: `member`

### Declared structure

- <a id="s-a2cee59484"></a>`kind`: `"method"`
- <a id="s-ab7d3b0128"></a>`signature`: `"\"(self, plan: 'WorkflowPlan', selections: 'dict[str, ArtifactSelection]') -> 'TargetPreflightRequest'\""`

## Maintained corroboration

### Related interface records

- [PlanningPort](stove0-core-planningport.md)

## Governing policies

- <a id="pa-f634c3cef5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PlanningPort.target_preflight_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87ef10352184e5d148b9c6f4ce3e662c74c7d0ac606054455619c138eacc4edf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan: 'WorkflowPlan', selections: 'dict[str, ArtifactSelection]') -> 'TargetPreflightRequest'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_preflight_request",
  "owner": "stove0_core.PlanningPort",
  "unit": "member"
}
```

</details>
