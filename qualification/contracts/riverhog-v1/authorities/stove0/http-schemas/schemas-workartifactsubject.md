# schemas: WorkArtifactSubject

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workartifactsubject:2c4da37eb3 -->

A collection logical file assigned an ID and role within one Stove0 work.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-42d670e575"></a>

- <a id="s-d9e0c3b904"></a>`type`: `"object"`
- <a id="s-894b658a8a"></a>`additionalProperties`: `false`
- <a id="s-bcfea8668a"></a>`description`: `"A collection logical file assigned an ID and role within one Stove0 work."`
- <a id="s-89e03fa8e5"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-e4a5f28a2b"></a>`title`: `"WorkArtifactSubject"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-113dae831c"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-d7182c2dc6"></a>`collection` | yes | [CollectionRootIdentityRef](schemas-collectionrootidentityref.md) |  |
| <a id="s-e8506c7dc4"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-d97d9b17d6"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; title="Media Type" |  |
| <a id="s-7e8e58870f"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-827be6b5a1"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-9f75d0e2ff"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-ced8e05f2e"></a>[field media_type · string value](#s-d97d9b17d6) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field path](#s-7e8e58870f) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field sha256](#s-9f75d0e2ff) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5d7ea236d3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ffd4553c9b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkArtifactSubject`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63b4f10bcf841dafdca74f8abcd719cec065928cd758bd20fd88b73407c64ec5 -->

```json
{
  "additionalProperties": false,
  "description": "A collection logical file assigned an ID and role within one Stove0 work.",
  "properties": {
    "bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    },
    "collection": {
      "$ref": "#/components/schemas/CollectionRootIdentityRef"
    },
    "id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "media_type": {
      "anyOf": [
        {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Media Type"
    },
    "path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Path",
      "type": "string"
    },
    "role": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Role",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "id",
    "role",
    "collection",
    "path",
    "bytes",
    "sha256"
  ],
  "title": "WorkArtifactSubject",
  "type": "object"
}
```

</details>
