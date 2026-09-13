# stove0_api.Stove0Composition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-api-stove0composition:55ec640f85 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b7594b8bd"></a>
| Field | Shape |
|---|---|
| <a id="s-773fc5618d"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-92764ff489"></a>`distribution` | "stove0-server" |
| <a id="s-fb4a15cf55"></a>`module` | "stove0_api" |
| <a id="s-3658a19a5b"></a>`name` | "Stove0Composition" |
| <a id="s-3a6c049120"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_api.Stove0Composition.build](stove0-api-stove0composition-build.md)

## Governing policies

- <a id="pa-e50864aaaf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_api](../../../evidence/sources.md#src-d5a12e8c56) — `reference/stove0/application/server/src/stove0_api/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api.Stove0Composition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50d95f28ac7e4bd3c8b104f18c2750a0715a0774017468a6e19ccacc33300c4b -->

```json
{
  "contract": {
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
    "signature": "\"(config: 'Stove0RuntimeConfig', riverhog_api: 'ApiClient', state: 'SqlAlchemyStateStore', recipes: 'RecipeCatalog', work: 'Stove0WorkService', coordinator: 'Stove0Coordinator', preview: 'WorkflowPreviewService', evaluations: 'EvaluationService', scheduler: 'Stove0Scheduler', admission: 'ClassificationAdmissionService | None' = None, target_callbacks: 'TargetCallbackAuthority | None' = None, browse_tokens: 'BrowseTokenCodec | None' = None) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_api",
  "name": "Stove0Composition",
  "unit": "export"
}
```
