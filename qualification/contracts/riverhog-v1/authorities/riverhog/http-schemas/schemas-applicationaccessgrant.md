# schemas: ApplicationAccessGrant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-applicationaccessgrant:cab3f8299a -->

One canonical public application-access request or response grant.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ee1b3c97d7"></a>

- <a id="s-2c00789161"></a>`type`: `"object"`
- <a id="s-ce5d8d75f5"></a>`additionalProperties`: `false`
- <a id="s-5f6a2a8d90"></a>`description`: `"One canonical public application-access request or response grant."`
- <a id="s-7bed041f7b"></a>`required`: `["permission"]`
- <a id="s-bdfdb13bfb"></a>`title`: `"ApplicationAccessGrant"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2cd54ec86"></a>`permission` | yes | [ApplicationPermission](schemas-applicationpermission.md) |  |
| <a id="s-ea597f207a"></a>`resource` | no | [ApplicationResource](schemas-applicationresource.md); default="*" |  |

### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1](#s-2ccce8339b) |

### <a id="s-2ccce8339b"></a>`allOf` alternative 1


#### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1 · `oneOf` alternative 1](#s-7107962cf4) |
| 2 | [See `allOf` alternative 1 · `oneOf` alternative 2](#s-4c880580ed) |
| 3 | [See `allOf` alternative 1 · `oneOf` alternative 3](#s-7d157e5c2d) |
| 4 | [See `allOf` alternative 1 · `oneOf` alternative 4](#s-fbcd7b5b19) |

### <a id="s-7107962cf4"></a>`allOf` alternative 1 · `oneOf` alternative 1

- <a id="s-42de5767ff"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-23290cbe92"></a>`permission` | yes | const="*" |  |
| <a id="s-c42a16c078"></a>`resource` | no | const="*" |  |

### <a id="s-4c880580ed"></a>`allOf` alternative 1 · `oneOf` alternative 2

- <a id="s-9299b16a53"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-996525436e"></a>`permission` | yes | const="collections:create" |  |
| <a id="s-d33ebe3dd1"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+)$" |  |

### <a id="s-7d157e5c2d"></a>`allOf` alternative 1 · `oneOf` alternative 3

- <a id="s-9af7e1bc5a"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b0a864b05"></a>`permission` | yes | enum=["archives:manage","archives:read","catalog:read","collection-descriptions:manage","collection-tags:manage","collections:delete","provenance:export","provenance:read","retrieval:manage"] |  |
| <a id="s-9758c5cd01"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$" |  |

### <a id="s-fbcd7b5b19"></a>`allOf` alternative 1 · `oneOf` alternative 4

- <a id="s-2040edf3f2"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e49c189b6a"></a>`permission` | yes | enum=["collection-transforms:control","collection-transforms:execute","events:read","events:read_all","keys:manage","quotas:manage"] |  |
| <a id="s-f29b6433d2"></a>`resource` | no | const="*" |  |

## Maintained corroboration

### Referenced contract dossiers

- [ApplicationPermission](schemas-applicationpermission.md)
- [ApplicationResource](schemas-applicationresource.md)

## Governing policies

- <a id="pa-436687b57f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationAccessGrant`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a73b17eccc32820fbe81fb1c4cb779b83cc4fcd6728aa1f4ca5e5b8223d8484 -->

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
  "description": "One canonical public application-access request or response grant.",
  "properties": {
    "permission": {
      "$ref": "#/components/schemas/ApplicationPermission"
    },
    "resource": {
      "$ref": "#/components/schemas/ApplicationResource",
      "default": "*"
    }
  },
  "required": [
    "permission"
  ],
  "title": "ApplicationAccessGrant",
  "type": "object"
}
```

</details>
