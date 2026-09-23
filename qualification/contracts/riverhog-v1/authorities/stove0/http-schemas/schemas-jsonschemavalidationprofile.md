# schemas: JsonSchemaValidationProfile

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-jsonschemavalidationprofile:d6b1933ee5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f13e08fabb"></a>

- <a id="s-c2d16948f3"></a>`type`: `"object"`
- <a id="s-945200b01b"></a>`additionalProperties`: `false`
- <a id="s-8d55cddea4"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-5ae1fa48cb"></a>`title`: `"JsonSchemaValidationProfile"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2d1d2be519"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-7249b6d020"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-b6e9b0728c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-7012e07ba0"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-dc8c398f1c"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Schema" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field schema](#s-dc8c398f1c) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field profile_sha256](#s-7012e07ba0) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b65de14345"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-1d478ed654"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-dbe3711acd"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JsonSchemaValidationProfile`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce6d4c868aab299ac4a63911f3cc0809af37e0a18437cc428b33061e3220a5a8 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "dialect": {
      "const": "https://json-schema.org/draft/2020-12/schema",
      "default": "https://json-schema.org/draft/2020-12/schema",
      "title": "Dialect",
      "type": "string"
    },
    "format_policy": {
      "const": "annotation-only",
      "default": "annotation-only",
      "title": "Format Policy",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "profile_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Profile Sha256",
      "type": "string"
    },
    "schema": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Schema",
      "type": "object"
    }
  },
  "required": [
    "id",
    "profile_sha256",
    "schema"
  ],
  "title": "JsonSchemaValidationProfile",
  "type": "object"
}
```

</details>
