# schemas: DepartureEffectIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-departureeffectintent:a50d83b1ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7549e86828"></a>

- <a id="s-926d395db2"></a>`type`: `"object"`
- <a id="s-7b868268b3"></a>`additionalProperties`: `false`
- <a id="s-5506973758"></a>`required`: `["policy_id","policy_revision","policy_sha256","target_registration_id","target_identity","source_identity","authorization_view_identity","last_collection","departure_cause","departure_revision","departure_id"]`
- <a id="s-59209907e5"></a>`title`: `"DepartureEffectIntent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f3a1964d8b"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Authorization View Identity" |  |
| <a id="s-b1583ef7a6"></a>`departure_cause` | yes | type="string"; enum=["collection_deleted","visibility_lost"]; title="Departure Cause" |  |
| <a id="s-88e16a20b2"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Departure Id" |  |
| <a id="s-8ebd9ba1c4"></a>`departure_revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"; title="Departure Revision" |  |
| <a id="s-a89fd19d58"></a>`format` | no | type="string"; const="stove0-departure-effect-intent/v1"; default="stove0-departure-effect-intent/v1"; title="Format" |  |
| <a id="s-2bc04be827"></a>`last_collection` | yes | [CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md) |  |
| <a id="s-bb76f2cec4"></a>`policy_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Policy Id" |  |
| <a id="s-53bd3673e7"></a>`policy_revision` | yes | type="integer"; minimum=1; title="Policy Revision" |  |
| <a id="s-def32f1a61"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Policy Sha256" |  |
| <a id="s-139c1177f1"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Source Identity" |  |
| <a id="s-be3bc5e8a7"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Identity" |  |
| <a id="s-741b879108"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1; title="Target Registration Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field authorization_view_identity](#s-f3a1964d8b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field departure_id](#s-88e16a20b2) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field departure_revision](#s-8ebd9ba1c4) | `length · characters · contract_max` | maximum=19; minimum=1; reason="schema-maximum" |
| [field policy_sha256](#s-def32f1a61) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field source_identity](#s-139c1177f1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field target_identity](#s-be3bc5e8a7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field target_registration_id](#s-741b879108) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [CatalogSyncDescriptor](schemas-catalogsyncdescriptor.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-433ccc5db8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ad76df19dd"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/DepartureEffectIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13883650a38eccfad679df7213221892341367d16ff04eee1bdf2b37020f6b58 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authorization_view_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Authorization View Identity",
      "type": "string"
    },
    "departure_cause": {
      "enum": [
        "collection_deleted",
        "visibility_lost"
      ],
      "title": "Departure Cause",
      "type": "string"
    },
    "departure_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Departure Id",
      "type": "string"
    },
    "departure_revision": {
      "maxLength": 19,
      "minLength": 1,
      "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
      "title": "Departure Revision",
      "type": "string"
    },
    "format": {
      "const": "stove0-departure-effect-intent/v1",
      "default": "stove0-departure-effect-intent/v1",
      "title": "Format",
      "type": "string"
    },
    "last_collection": {
      "$ref": "#/components/schemas/CatalogSyncDescriptor"
    },
    "policy_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
      "title": "Policy Id",
      "type": "string"
    },
    "policy_revision": {
      "minimum": 1,
      "title": "Policy Revision",
      "type": "integer"
    },
    "policy_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Policy Sha256",
      "type": "string"
    },
    "source_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Identity",
      "type": "string"
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
    "policy_id",
    "policy_revision",
    "policy_sha256",
    "target_registration_id",
    "target_identity",
    "source_identity",
    "authorization_view_identity",
    "last_collection",
    "departure_cause",
    "departure_revision",
    "departure_id"
  ],
  "title": "DepartureEffectIntent",
  "type": "object"
}
```

</details>
