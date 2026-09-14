# stove0_core.ClassificationAdmissionService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-classificationadmissionservice:2ca6a19ab0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-32befd4918"></a>
- <a id="s-85ea72eff0"></a>`distribution`: `stove0-server`
- <a id="s-bf3981d9a3"></a>`module`: `stove0_core`
- <a id="s-381d1f52a0"></a>`name`: `ClassificationAdmissionService`
- <a id="s-66c2d736be"></a>`unit`: `export`

### Declared structure

- <a id="s-1fe9a53d10"></a>`kind`: `"class"`
- <a id="s-f9253be290"></a>`signature`: `"\"(*, catalog: 'AdmissionCatalog', riverhog: 'ApiClient', state: 'SqlAlchemyStateStore', planner: 'RecipePlanner', preview: 'WorkflowPreviewService', coordinator: 'Stove0Coordinator') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.ClassificationAdmissionService.get_admission](stove0-core-classificationadmissionservice-get-admission.md)
- [stove0_core.ClassificationAdmissionService.rebaseline](stove0-core-classificationadmissionservice-rebaseline.md)
- [stove0_core.ClassificationAdmissionService.list_admissions](stove0-core-classificationadmissionservice-list-admissions.md)
- [stove0_core.ClassificationAdmissionService.advance](stove0-core-classificationadmissionservice-advance.md)
- [stove0_core.ClassificationAdmissionService.policies](stove0-core-classificationadmissionservice-policies.md)

## Governing policies

- <a id="pa-332de24a44"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ClassificationAdmissionService`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e425cfbd54558e6c429397f15961c373f3fa09b22a9cf2a0020b0f9ca1b72581 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, catalog: 'AdmissionCatalog', riverhog: 'ApiClient', state: 'SqlAlchemyStateStore', planner: 'RecipePlanner', preview: 'WorkflowPreviewService', coordinator: 'Stove0Coordinator') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ClassificationAdmissionService",
  "unit": "export"
}
```
