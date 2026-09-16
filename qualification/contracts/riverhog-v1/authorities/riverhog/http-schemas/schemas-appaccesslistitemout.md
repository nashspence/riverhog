# schemas: AppAccessListItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-appaccesslistitemout:2c1fa51506 -->

Exact externally visible contract owned by this semantic dossier.

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
| <a id="s-b38b195323"></a>`app` | yes | #/components/schemas/ApplicationName |  |
| <a id="s-4f422c3d19"></a>`created_at` | yes | type="string" |  |
| <a id="s-3323fdf105"></a>`key_id` | yes | #/components/schemas/ApplicationKeyId |  |
| <a id="s-3fb98e8404"></a>`key_status` | yes | type="string"; enum=["active","expired","revoked"] |  |
| <a id="s-f4a4193314"></a>`permission` | yes | #/components/schemas/ApplicationPermission |  |
| <a id="s-a40b921ce5"></a>`resource` | no | $ref="#/components/schemas/ApplicationResource"; default="*" |  |

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
| <a id="s-99d8f46218"></a>`permission` | yes | enum=["collection-transforms:control","collection-transforms:execute","events:read","events:read_all","keys:manage","quotas:manage"] |  |
| <a id="s-8e2da8ff39"></a>`resource` | no | const="*" |  |

## Maintained corroboration

### Referenced contract dossiers

- [ApplicationKeyId](schemas-applicationkeyid.md)
- [ApplicationName](schemas-applicationname.md)
- [ApplicationPermission](schemas-applicationpermission.md)
- [ApplicationResource](schemas-applicationresource.md)

## Governing policies

- <a id="pa-b4137b2151"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListItemOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7bc29c0ddede1e5fd1ca2063d9f2dc96c470b99584e673394ed043ac5cb2c095 -->

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
                "collection-transforms:control",
                "collection-transforms:execute",
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
