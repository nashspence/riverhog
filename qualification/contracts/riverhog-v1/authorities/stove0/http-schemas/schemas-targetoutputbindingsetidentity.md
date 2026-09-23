# schemas: TargetOutputBindingSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetoutputbindingsetidentity:feee092817 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9eaf0f87ce"></a>

- <a id="s-64b260b804"></a>`type`: `"object"`
- <a id="s-5afc8e74e6"></a>`additionalProperties`: `false`
- <a id="s-8c08ab77f6"></a>`required`: `["artifact_count","total_bytes","sha256"]`
- <a id="s-0da11a55a9"></a>`title`: `"TargetOutputBindingSetIdentity"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7172839d4f"></a>`artifact_count` | yes | type="integer"; minimum=1; title="Artifact Count" |  |
| <a id="s-048703c7e4"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |
| <a id="s-895f414e6b"></a>`total_bytes` | yes | type="integer"; minimum=0; title="Total Bytes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-048703c7e4) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b93cdf4727"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e3aff49dee"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetOutputBindingSetIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: daf38f19dc312122935afd84d58bf963fa26e233d8633d1f209196e4f92d8e9a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifact_count": {
      "minimum": 1,
      "title": "Artifact Count",
      "type": "integer"
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
    "sha256"
  ],
  "title": "TargetOutputBindingSetIdentity",
  "type": "object"
}
```

</details>
