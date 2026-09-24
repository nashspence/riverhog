# schemas: CollectionTagMembershipOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiontagmembershipout:9be52487db -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-40640df4dd"></a>

- <a id="s-cd831e18f4"></a>`type`: `"object"`
- <a id="s-a70c174546"></a>`additionalProperties`: `false`
- <a id="s-be8855e621"></a>`required`: `["collection_id","revision","tag_set_identity","tag","present"]`
- <a id="s-4f33f32921"></a>`title`: `"CollectionTagMembershipOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-20b865a229"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-0ec7923b52"></a>`present` | yes | type="boolean"; title="Present" |  |
| <a id="s-a0e672fd9e"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; title="Revision" |  |
| <a id="s-977d735939"></a>`tag` | yes | [CollectionTag](schemas-collectiontag.md) |  |
| <a id="s-a9469d3ec3"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Tag Set Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field revision](#s-a0e672fd9e) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| [field tag_set_identity](#s-a9469d3ec3) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [CollectionTag](schemas-collectiontag.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-bae7089d2f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-29d83ac70b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagMembershipOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5738ba195894d75eda6ed2223cfb464e17eaa62270678415546f6c0f887dd89f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "present": {
      "title": "Present",
      "type": "boolean"
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    },
    "tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "revision",
    "tag_set_identity",
    "tag",
    "present"
  ],
  "title": "CollectionTagMembershipOut",
  "type": "object"
}
```

</details>
