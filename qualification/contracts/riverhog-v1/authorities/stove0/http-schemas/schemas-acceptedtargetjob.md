# schemas: AcceptedTargetJob

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-acceptedtargetjob:cf27bf6990 -->

Durable, non-secret identity of one accepted target job request.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0b3d1b3480"></a>

- <a id="s-b7f9ca6c75"></a>`type`: `"object"`
- <a id="s-979fe17e46"></a>`additionalProperties`: `false`
- <a id="s-ee57f72a82"></a>`description`: `"Durable, non-secret identity of one accepted target job request."`
- <a id="s-504c319471"></a>`required`: `["declaration","request_sha256"]`
- <a id="s-62ac17eb15"></a>`title`: `"AcceptedTargetJob"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9dcf35deb6"></a>`declaration` | yes | [TargetJobDeclaration](schemas-targetjobdeclaration.md) |  |
| <a id="s-e846f6a178"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Request Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field request_sha256](#s-e846f6a178) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [TargetJobDeclaration](schemas-targetjobdeclaration.md)

## Governing policies

- <a id="pa-1302e62af6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c2f6d108e6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AcceptedTargetJob`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f00e108b9ab996f28a1dfd3928c40b9045b480dd5240737de55621c6ceb42bd -->

```json
{
  "additionalProperties": false,
  "description": "Durable, non-secret identity of one accepted target job request.",
  "properties": {
    "declaration": {
      "$ref": "#/components/schemas/TargetJobDeclaration"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
      "type": "string"
    }
  },
  "required": [
    "declaration",
    "request_sha256"
  ],
  "title": "AcceptedTargetJob",
  "type": "object"
}
```

</details>
