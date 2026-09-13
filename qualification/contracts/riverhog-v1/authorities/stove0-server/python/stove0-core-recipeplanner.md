# stove0_core.RecipePlanner

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner:bcea0f21cd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-85e2915dba"></a>
| Field | Shape |
|---|---|
| <a id="s-7c2e268d4f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-01cfbe7387"></a>`distribution` | "stove0-server" |
| <a id="s-8ed2a24a98"></a>`module` | "stove0_core" |
| <a id="s-3cf98f175b"></a>`name` | "RecipePlanner" |
| <a id="s-430af5a5c7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipePlanner.create_work](stove0-core-recipeplanner-create-work.md)
- [stove0_core.RecipePlanner.observation_requests](stove0-core-recipeplanner-observation-requests.md)
- [stove0_core.RecipePlanner.operation_contract](stove0-core-recipeplanner-operation-contract.md)
- [stove0_core.RecipePlanner.target_input_selection](stove0-core-recipeplanner-target-input-selection.md)
- [stove0_core.RecipePlanner.target_preflight_request](stove0-core-recipeplanner-target-preflight-request.md)
- [stove0_core.RecipePlanner.workflow_plan](stove0-core-recipeplanner-workflow-plan.md)

## Governing policies

- <a id="pa-734ff5162d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner`

### Exact owned JSON

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
