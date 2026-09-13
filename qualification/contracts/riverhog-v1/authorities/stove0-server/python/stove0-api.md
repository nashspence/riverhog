# stove0_api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-api:537cc77a41 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-97e3cc8c60"></a>
| Field | Shape |
|---|---|
| <a id="s-8d81654e4b"></a>`candidate_id` | "python:stove0-server:stove0_api" |
| <a id="s-83670ce17b"></a>`distribution` | "stove0-server" |
| <a id="s-2f10c546c9"></a>`exports` | additional keys=`Stove0Composition`, `create_app` |
| <a id="s-e7ab9393d9"></a>`module` | "stove0_api" |

## Governing policies

- <a id="pa-5fceb87ec7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_api](../../../evidence/sources.md#src-d5a12e8c56) — `reference/stove0/application/server/src/stove0_api/__init__.py`

### Machine authority

- `/external_contract/python/57`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33eea8a41a71af2212d9f66d548747e6d676090044bdccf36bf3712ea177b40d -->

```json
{
  "candidate_id": "python:stove0-server:stove0_api",
  "distribution": "stove0-server",
  "exports": {
    "Stove0Composition": {
      "fields": [
        {
          "default": "required",
          "name": "config",
          "type": "'Stove0RuntimeConfig'"
        },
        {
          "default": "required",
          "name": "riverhog_api",
          "type": "'ApiClient'"
        },
        {
          "default": "required",
          "name": "state",
          "type": "'SqlAlchemyStateStore'"
        },
        {
          "default": "required",
          "name": "recipes",
          "type": "'RecipeCatalog'"
        },
        {
          "default": "required",
          "name": "work",
          "type": "'Stove0WorkService'"
        },
        {
          "default": "required",
          "name": "coordinator",
          "type": "'Stove0Coordinator'"
        },
        {
          "default": "required",
          "name": "preview",
          "type": "'WorkflowPreviewService'"
        },
        {
          "default": "required",
          "name": "evaluations",
          "type": "'EvaluationService'"
        },
        {
          "default": "required",
          "name": "scheduler",
          "type": "'Stove0Scheduler'"
        },
        {
          "default": "None",
          "name": "admission",
          "type": "'ClassificationAdmissionService | None'"
        },
        {
          "default": "None",
          "name": "target_callbacks",
          "type": "'TargetCallbackAuthority | None'"
        },
        {
          "default": "None",
          "name": "browse_tokens",
          "type": "'BrowseTokenCodec | None'"
        }
      ],
      "kind": "class",
      "members": {
        "build": {
          "kind": "classmethod",
          "signature": "\"(cls, config: 'Stove0RuntimeConfig') -> 'Stove0Composition'\""
        }
      },
      "signature": "\"(config: 'Stove0RuntimeConfig', riverhog_api: 'ApiClient', state: 'SqlAlchemyStateStore', recipes: 'RecipeCatalog', work: 'Stove0WorkService', coordinator: 'Stove0Coordinator', preview: 'WorkflowPreviewService', evaluations: 'EvaluationService', scheduler: 'Stove0Scheduler', admission: 'ClassificationAdmissionService | None' = None, target_callbacks: 'TargetCallbackAuthority | None' = None, browse_tokens: 'BrowseTokenCodec | None' = None) -> None\""
    },
    "create_app": {
      "kind": "function",
      "signature": "\"(composition: 'Stove0Composition') -> 'FastAPI'\""
    }
  },
  "module": "stove0_api"
}
```
