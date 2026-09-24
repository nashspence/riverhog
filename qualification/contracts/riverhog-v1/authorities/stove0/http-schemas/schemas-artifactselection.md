# schemas: ArtifactSelection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-artifactselection:1ed2ea4dec -->

One exact, content-addressed selection of immutable artifacts.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b71d8b7268"></a>

- <a id="s-d52f6dc451"></a>`type`: `"object"`
- <a id="s-4084b51d43"></a>`additionalProperties`: `false`
- <a id="s-413c8d021c"></a>`description`: `"One exact, content-addressed selection of immutable artifacts."`
- <a id="s-6c071c3959"></a>`required`: `["artifacts","artifact_count","total_bytes","selection_sha256"]`
- <a id="s-24a1678880"></a>`title`: `"ArtifactSelection"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ca1a027a8"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-718513cbc7"></a>`artifacts` | yes | type="array"; items=([WorkArtifactSubject](schemas-workartifactsubject.md)); minItems=1; title="Artifacts" |  |
| <a id="s-bb0cdcf29c"></a>`format` | no | type="string"; const="stove0-artifact-selection/v1"; default="stove0-artifact-selection/v1"; title="Format" |  |
| <a id="s-a95861fb35"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-c25a38133a"></a>`total_bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-718513cbc7) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field selection_sha256](#s-a95861fb35) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [WorkArtifactSubject](schemas-workartifactsubject.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-bed7fde96d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-885e0fd2b3"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-b3015e8d39"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSelection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62a0a3b10d9b09be956e33a9910acb4d536c0f9a86c5666681fbebfb762cdef8 -->

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
        "$ref": "#/components/schemas/WorkArtifactSubject"
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
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
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

</details>
