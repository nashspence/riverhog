# schemas: DeparturePolicyStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-departurepolicystatus:9e789fe467 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e562ebeb86"></a>

- <a id="s-f052fd9375"></a>`type`: `"object"`
- <a id="s-9bc5b3c726"></a>`additionalProperties`: `false`
- <a id="s-44db4cc647"></a>`required`: `["policy","policy_sha256","phase","through_revision","updated_at"]`
- <a id="s-5a59f4764a"></a>`title`: `"DeparturePolicyStatus"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-204a2acf72"></a>`authorization_view_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Authorization View Identity" |  |
| <a id="s-684f191995"></a>`phase` | yes | type="string"; enum=["new","baseline","following","reset_required"]; title="Phase" |  |
| <a id="s-4dd78e831d"></a>`policy` | yes | [DeparturePolicy](schemas-departurepolicy.md) |  |
| <a id="s-3a5972c38f"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Policy Sha256" |  |
| <a id="s-37bd97859f"></a>`source_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Source Identity" |  |
| <a id="s-a3da1a1eeb"></a>`through_revision` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]*)$"; title="Through Revision" |  |
| <a id="s-7dc746f34d"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Updated At" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-125fe57067"></a>[field authorization_view_identity · string value](#s-204a2acf72) | `length · characters · fixed` | maximum=64; minimum=64; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field policy_sha256](#s-3a5972c38f) | `length · characters · fixed` | maximum=64; minimum=64; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-5a7b3f36a4"></a>[field source_identity · string value](#s-37bd97859f) | `length · characters · fixed` | maximum=64; minimum=64; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field updated_at](#s-7dc746f34d) | `length · characters · fixed` | maximum=30; minimum=30 |

## Maintained corroboration

### Referenced contract elements

- [DeparturePolicy](schemas-departurepolicy.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-bfaefa698c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-5c0abfea56"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/DeparturePolicyStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cbf38a977c146c6bcedd11b2789a3848a9c8413055b7656f7399ab84a23a597 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authorization_view_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Authorization View Identity"
    },
    "phase": {
      "enum": [
        "new",
        "baseline",
        "following",
        "reset_required"
      ],
      "title": "Phase",
      "type": "string"
    },
    "policy": {
      "$ref": "#/components/schemas/DeparturePolicy"
    },
    "policy_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Policy Sha256",
      "type": "string"
    },
    "source_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Source Identity"
    },
    "through_revision": {
      "pattern": "^(?:0|[1-9][0-9]*)$",
      "title": "Through Revision",
      "type": "string"
    },
    "updated_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Updated At",
      "type": "string"
    }
  },
  "required": [
    "policy",
    "policy_sha256",
    "phase",
    "through_revision",
    "updated_at"
  ],
  "title": "DeparturePolicyStatus",
  "type": "object"
}
```

</details>
