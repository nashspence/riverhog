# schemas: OutputArtifactSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-outputartifactsetidentity:554653c0fa -->

Small identity for target outputs already registered with Riverhog.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-1680fd38ae"></a>
- <a id="s-2069759423"></a>`title`: OutputArtifactSetIdentity
- <a id="s-e13b763a35"></a>`description`: Small identity for target outputs already registered with Riverhog.
- <a id="s-0edbb796f7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5990b9f86c"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-e4d192a5f2"></a>`roles` | yes | type="array"; minItems=1; items=(#/components/schemas/OutputArtifactRoleCount) |  |
| <a id="s-a86f446c9b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d7f31811ef"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field roles](#s-e4d192a5f2) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-a86f446c9b) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: OutputArtifactRoleCount](schemas-outputartifactrolecount.md)

## Governing policies

- <a id="pa-d7605ef0d9"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4a9b2e817d"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-100d0caf4a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OutputArtifactSetIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b8358c7d637b9865f6fa839ce95492058b0b03b126cf898f334be2dd855020c -->

```json
{
  "additionalProperties": false,
  "description": "Small identity for target outputs already registered with Riverhog.",
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
    },
    "roles": {
      "items": {
        "$ref": "#/components/schemas/OutputArtifactRoleCount"
      },
      "minItems": 1,
      "title": "Roles",
      "type": "array"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "artifact_count",
    "total_bytes",
    "roles",
    "sha256"
  ],
  "title": "OutputArtifactSetIdentity",
  "type": "object"
}
```
