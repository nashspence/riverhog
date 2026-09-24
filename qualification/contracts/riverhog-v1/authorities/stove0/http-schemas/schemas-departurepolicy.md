# schemas: DeparturePolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-departurepolicy:c99af8a98a -->

A catalog departure subscription with no recipe or artifact authority.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5120202567"></a>

- <a id="s-3edb3aaff9"></a>`type`: `"object"`
- <a id="s-2758cdc12b"></a>`additionalProperties`: `false`
- <a id="s-a675c6f8a8"></a>`description`: `"A catalog departure subscription with no recipe or artifact authority."`
- <a id="s-df2e459755"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`
- <a id="s-f4c3e29a57"></a>`title`: `"DeparturePolicy"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a3de0a69e9"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1"; title="Format" |  |
| <a id="s-17e3cb4e44"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-5c89f069f0"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-d40d477b04"></a>`selector` | yes | discriminator={"mapping":{"all":"#/components/schemas/AllVisibleAdmissionSelector","tags":"#/components/schemas/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](schemas-allvisibleadmissionselector.md)); ([TaggedAdmissionSelector](schemas-taggedadmissionselector.md))]; title="Selector" |  |
| <a id="s-67a7d13104"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Identity" |  |
| <a id="s-1da3c9b897"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1; title="Target Registration Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field target_identity](#s-67a7d13104) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field target_registration_id](#s-1da3c9b897) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [AllVisibleAdmissionSelector](schemas-allvisibleadmissionselector.md)
- [TaggedAdmissionSelector](schemas-taggedadmissionselector.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-14ce11fd88"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-1b9999a799"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/DeparturePolicy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6305053133f6cc25a60d36643bcbc84861d5ff219a1ce33f11f0f243143d4b5c -->

```json
{
  "additionalProperties": false,
  "description": "A catalog departure subscription with no recipe or artifact authority.",
  "properties": {
    "format": {
      "const": "stove0-departure-policy/v1",
      "default": "stove0-departure-policy/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "selector": {
      "discriminator": {
        "mapping": {
          "all": "#/components/schemas/AllVisibleAdmissionSelector",
          "tags": "#/components/schemas/TaggedAdmissionSelector"
        },
        "propertyName": "kind"
      },
      "oneOf": [
        {
          "$ref": "#/components/schemas/AllVisibleAdmissionSelector"
        },
        {
          "$ref": "#/components/schemas/TaggedAdmissionSelector"
        }
      ],
      "title": "Selector"
    },
    "target_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Identity",
      "type": "string"
    },
    "target_registration_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Target Registration Id",
      "type": "string"
    }
  },
  "required": [
    "id",
    "revision",
    "selector",
    "target_registration_id",
    "target_identity"
  ],
  "title": "DeparturePolicy",
  "type": "object"
}
```

</details>
