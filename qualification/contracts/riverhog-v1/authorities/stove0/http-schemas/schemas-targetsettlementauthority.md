# schemas: TargetSettlementAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetsettlementauthority:e58fbd9b92 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-428567adad"></a>

- <a id="s-49261af54d"></a>`type`: `"object"`
- <a id="s-dc7c14f671"></a>`additionalProperties`: `false`
- <a id="s-1015e80964"></a>`required`: `["job_id","production_sha256","output_collection","output_bindings","settlement_sha256"]`
- <a id="s-6bb82eba89"></a>`title`: `"TargetSettlementAuthority"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea9a996c0a"></a>`format` | no | type="string"; const="stove0-target-settlement/v1"; default="stove0-target-settlement/v1"; title="Format" |  |
| <a id="s-cde8f44573"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-55ad886411"></a>`output_bindings` | yes | [TargetOutputBindingSetIdentity](schemas-targetoutputbindingsetidentity.md) |  |
| <a id="s-4b257ebb6b"></a>`output_collection` | yes | [OutputCollectionRef](schemas-outputcollectionref.md) |  |
| <a id="s-525e441ceb"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Production Sha256" |  |
| <a id="s-3bfcddaed7"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field job_id](#s-cde8f44573) | `length · characters · fixed` | shared above |
| [field production_sha256](#s-525e441ceb) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-3bfcddaed7) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [OutputCollectionRef](schemas-outputcollectionref.md)
- [TargetOutputBindingSetIdentity](schemas-targetoutputbindingsetidentity.md)

## Governing policies

- <a id="pa-d482c61c49"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d680e303c2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetSettlementAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a811b1b88f435f8192083bd5bb9cab621322a012a9d17a39beebcbf46f0b2e02 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-target-settlement/v1",
      "default": "stove0-target-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "output_bindings": {
      "$ref": "#/components/schemas/TargetOutputBindingSetIdentity"
    },
    "output_collection": {
      "$ref": "#/components/schemas/OutputCollectionRef"
    },
    "production_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Production Sha256",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "production_sha256",
    "output_collection",
    "output_bindings",
    "settlement_sha256"
  ],
  "title": "TargetSettlementAuthority",
  "type": "object"
}
```

</details>
