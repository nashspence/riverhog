# stove0_core.RecipePlanner

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner:bcea0f21cd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-85e2915dba"></a>
- <a id="s-01cfbe7387"></a>`distribution`: `stove0-server`
- <a id="s-8ed2a24a98"></a>`module`: `stove0_core`
- <a id="s-3cf98f175b"></a>`name`: `RecipePlanner`
- <a id="s-430af5a5c7"></a>`unit`: `export`

### Declared structure

- <a id="s-9546d3f424"></a>`kind`: `"class"`
- <a id="s-eeee34cd55"></a>`signature`: `"\"(*, catalog: 'RecipeCatalog', riverhog: 'ApiClient', observers: 'ObserverPort', targets: 'TargetPort') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [create_work](stove0-core-recipeplanner-create-work.md)
- [observation_requests](stove0-core-recipeplanner-observation-requests.md)
- [operation_contract](stove0-core-recipeplanner-operation-contract.md)
- [target_input_selection](stove0-core-recipeplanner-target-input-selection.md)
- [target_preflight_request](stove0-core-recipeplanner-target-preflight-request.md)
- [workflow_plan](stove0-core-recipeplanner-workflow-plan.md)

## Governing policies

- <a id="pa-734ff5162d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c05f3bfc63e8f268fb64ac432b789c216ca2013743af6e38962444144e607ad0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, catalog: 'RecipeCatalog', riverhog: 'ApiClient', observers: 'ObserverPort', targets: 'TargetPort') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RecipePlanner",
  "unit": "export"
}
```

</details>
