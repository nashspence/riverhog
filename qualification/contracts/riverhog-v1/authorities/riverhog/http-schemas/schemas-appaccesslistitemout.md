# schemas: AppAccessListItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-appaccesslistitemout:2c1fa51506 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3254e40cf4"></a>

- <a id="s-90082cfd17"></a>`type`: `"object"`
- <a id="s-d38173e459"></a>`additionalProperties`: `false`
- <a id="s-09232691d4"></a>`required`: `["permission","app","key_id","key_status","created_at"]`
- <a id="s-250a15cee0"></a>`title`: `"AppAccessListItemOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b38b195323"></a>`app` | yes | [ApplicationName](schemas-applicationname.md) |  |
| <a id="s-4f422c3d19"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Created At" |  |
| <a id="s-3323fdf105"></a>`key_id` | yes | [ApplicationKeyId](schemas-applicationkeyid.md) |  |
| <a id="s-3fb98e8404"></a>`key_status` | yes | type="string"; enum=["active","expired","revoked"]; title="Key Status" |  |
| <a id="s-f4a4193314"></a>`permission` | yes | [ApplicationPermission](schemas-applicationpermission.md) |  |
| <a id="s-a40b921ce5"></a>`resource` | no | [ApplicationResource](schemas-applicationresource.md); default="*" |  |

### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1](#s-5cd9df2132) |

### <a id="s-5cd9df2132"></a>`allOf` alternative 1


#### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1 · `oneOf` alternative 1](#s-6b98e6a89a) |
| 2 | [See `allOf` alternative 1 · `oneOf` alternative 2](#s-06abadca6c) |
| 3 | [See `allOf` alternative 1 · `oneOf` alternative 3](#s-b6fce70083) |
| 4 | [See `allOf` alternative 1 · `oneOf` alternative 4](#s-8d9f16ce16) |

### <a id="s-6b98e6a89a"></a>`allOf` alternative 1 · `oneOf` alternative 1

- <a id="s-a01614d8c1"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4d17c0d196"></a>`permission` | yes | const="*" |  |
| <a id="s-ac46cfaa56"></a>`resource` | no | const="*" |  |

### <a id="s-06abadca6c"></a>`allOf` alternative 1 · `oneOf` alternative 2

- <a id="s-211de8d13e"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-720edc8213"></a>`permission` | yes | const="collections:create" |  |
| <a id="s-be8b95498a"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+)$" |  |

### <a id="s-b6fce70083"></a>`allOf` alternative 1 · `oneOf` alternative 3

- <a id="s-b8a5cb69df"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ba2fbabfc"></a>`permission` | yes | enum=["archives:manage","archives:read","catalog:read","collection-descriptions:manage","collection-tags:manage","collections:delete","provenance:export","provenance:read","retrieval:manage"] |  |
| <a id="s-e143793e8a"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$" |  |

### <a id="s-8d9f16ce16"></a>`allOf` alternative 1 · `oneOf` alternative 4

- <a id="s-bba35cd1b1"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-99d8f46218"></a>`permission` | yes | enum=["collection-processing:control","collection-processing:execute","events:read","events:read_all","keys:manage","quotas:manage"] |  |
| <a id="s-8e2da8ff39"></a>`resource` | no | const="*" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field created_at](#s-4f422c3d19) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ApplicationKeyId](schemas-applicationkeyid.md)
- [ApplicationName](schemas-applicationname.md)
- [ApplicationPermission](schemas-applicationpermission.md)
- [ApplicationResource](schemas-applicationresource.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b4137b2151"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c7825e91cf"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListItemOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 143213c593ed4b71b1d6b1daae5321f1f4fc469b3d1eb7f083d21a34a99ca4cf -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "oneOf": [
        {
          "properties": {
            "permission": {
              "const": "*"
            },
            "resource": {
              "const": "*"
            }
          },
          "required": [
            "permission"
          ]
        },
        {
          "properties": {
            "permission": {
              "const": "collections:create"
            },
            "resource": {
              "pattern": "^(?:\\*|tag:.+)$",
              "type": "string"
            }
          },
          "required": [
            "permission"
          ]
        },
        {
          "properties": {
            "permission": {
              "enum": [
                "archives:manage",
                "archives:read",
                "catalog:read",
                "collection-descriptions:manage",
                "collection-tags:manage",
                "collections:delete",
                "provenance:export",
                "provenance:read",
                "retrieval:manage"
              ]
            },
            "resource": {
              "pattern": "^(?:\\*|tag:.+|collection:[1-9][0-9]*)$",
              "type": "string"
            }
          },
          "required": [
            "permission"
          ]
        },
        {
          "properties": {
            "permission": {
              "enum": [
                "collection-processing:control",
                "collection-processing:execute",
                "events:read",
                "events:read_all",
                "keys:manage",
                "quotas:manage"
              ]
            },
            "resource": {
              "const": "*"
            }
          },
          "required": [
            "permission"
          ]
        }
      ]
    }
  ],
  "properties": {
    "app": {
      "$ref": "#/components/schemas/ApplicationName"
    },
    "created_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Created At",
      "type": "string"
    },
    "key_id": {
      "$ref": "#/components/schemas/ApplicationKeyId"
    },
    "key_status": {
      "enum": [
        "active",
        "expired",
        "revoked"
      ],
      "title": "Key Status",
      "type": "string"
    },
    "permission": {
      "$ref": "#/components/schemas/ApplicationPermission"
    },
    "resource": {
      "$ref": "#/components/schemas/ApplicationResource",
      "default": "*"
    }
  },
  "required": [
    "permission",
    "app",
    "key_id",
    "key_status",
    "created_at"
  ],
  "title": "AppAccessListItemOut",
  "type": "object"
}
```

</details>
