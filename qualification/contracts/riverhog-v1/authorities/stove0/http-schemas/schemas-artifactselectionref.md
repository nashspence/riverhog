# schemas: ArtifactSelectionRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-artifactselectionref:d71579316d -->

Closed reference to a separately retained selection document.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-bd12209c43"></a>

- <a id="s-39aec049de"></a>`type`: `"object"`
- <a id="s-c61f70470c"></a>`additionalProperties`: `false`
- <a id="s-f5f68bd01b"></a>`description`: `"Closed reference to a separately retained selection document."`
- <a id="s-9517387a3f"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`
- <a id="s-512635ca2f"></a>`title`: `"ArtifactSelectionRef"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d3ec2665dd"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-77bdcdd02e"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Selection Sha256" |  |
| <a id="s-04c329f29f"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field selection_sha256](#s-77bdcdd02e) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-e9e83b40b1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c71cf56be1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSelectionRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8cd0eea202ee0c8f3d19552216aa1b0d50521d431df3a185e911bfe1c3aa4c7 -->

```json
{
  "additionalProperties": false,
  "description": "Closed reference to a separately retained selection document.",
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
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
    "selection_sha256",
    "artifact_count",
    "total_bytes"
  ],
  "title": "ArtifactSelectionRef",
  "type": "object"
}
```

</details>
