# schemas: InputArtifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-inputartifact:163b290c99 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6d70b97218"></a>

- <a id="s-dc1722292e"></a>`type`: `"object"`
- <a id="s-afd7794758"></a>`additionalProperties`: `false`
- <a id="s-aeeee36e47"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-5522727766"></a>`title`: `"InputArtifact"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-993c7b572e"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-2184fb8f86"></a>`collection` | yes | [CollectionRootIdentityRef](schemas-collectionrootidentityref.md) |  |
| <a id="s-0eebf2992b"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-441beb6169"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; title="Media Type" |  |
| <a id="s-b428e392ba"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-362483e0d8"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-9ed0ecdbca"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-bea144e121"></a>[field media_type · string value](#s-441beb6169) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field path](#s-b428e392ba) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field sha256](#s-9ed0ecdbca) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-77637713d2"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e572b9104d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/InputArtifact`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23a2f0df22393e58bdf39a7d098de6e6f36a8311cc6f173234dce51dfb5daab4 -->

```json
{
  "additionalProperties": false,
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
  "title": "InputArtifact",
  "type": "object"
}
```

</details>
