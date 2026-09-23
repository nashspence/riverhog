# schemas: OutputArtifactSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-outputartifactsetidentity:8f9423c573 -->

Small identity for target outputs already registered with Riverhog.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1680fd38ae"></a>

- <a id="s-0edbb796f7"></a>`type`: `"object"`
- <a id="s-d78a9ffdf4"></a>`additionalProperties`: `false`
- <a id="s-e13b763a35"></a>`description`: `"Small identity for target outputs already registered with Riverhog."`
- <a id="s-3e1bd7aa4b"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`
- <a id="s-2069759423"></a>`title`: `"OutputArtifactSetIdentity"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5990b9f86c"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-e4d192a5f2"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](schemas-outputartifactrolecount.md)); minItems=1; title="Roles" |  |
| <a id="s-a86f446c9b"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-d7f31811ef"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field roles](#s-e4d192a5f2) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-a86f446c9b) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [OutputArtifactRoleCount](schemas-outputartifactrolecount.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-60a191718e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-8efe6c6208"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-87acabf08b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OutputArtifactSetIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
