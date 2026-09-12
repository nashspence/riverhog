# schemas: ArtifactSelection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactselection:0876c0940c -->

One exact, content-addressed selection of immutable artifacts.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-b71d8b7268e3"></a>
- <a id="s-24a1678880d9"></a>`title`: ArtifactSelection
- <a id="s-413c8d021c14"></a>`description`: One exact, content-addressed selection of immutable artifacts.
- <a id="s-d52f6dc4514d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ca1a027a8a0"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-718513cbc726"></a>`artifacts` | yes | type="array"; minItems=1; items=(#/components/schemas/ArtifactSubject) |  |
| <a id="s-bb0cdcf29c6e"></a>`format` | no | type="string"; const="stove0-artifact-selection/v1" |  |
| <a id="s-a95861fb3547"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c25a38133a96"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-718513cbc726) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field selection_sha256](#s-a95861fb3547) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSubject](schemas-artifactsubject.md)

## Governing policies

- <a id="pa-feb361d5fdd7"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-b9a958b2cffd"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-195e0fcec4c8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSelection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8af48804a1c059e6b44aa3f4ab91efe8fccf666f2e8c8f0bc454359fb12b95ca -->

```json
{
  "additionalProperties": false,
  "description": "One exact, content-addressed selection of immutable artifacts.",
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
    },
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/ArtifactSubject"
      },
      "minItems": 1,
      "title": "Artifacts",
      "type": "array"
    },
    "format": {
      "const": "stove0-artifact-selection/v1",
      "default": "stove0-artifact-selection/v1",
      "title": "Format",
      "type": "string"
    },
    "selection_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Selection Sha256",
      "type": "string"
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "artifacts",
    "artifact_count",
    "total_bytes",
    "selection_sha256"
  ],
  "title": "ArtifactSelection",
  "type": "object"
}
```
