# schemas: MutateAppAccessRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-mutateappaccessrequest:6e9cd5d009 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3ba8013e13"></a>

- <a id="s-9d31a8ba7a"></a>`type`: `"object"`
- <a id="s-66216a25df"></a>`additionalProperties`: `false`
- <a id="s-3a6933cfeb"></a>`required`: `["permission"]`
- <a id="s-433bff20f9"></a>`title`: `"MutateAppAccessRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cedbc6789a"></a>`permission` | yes | [ApplicationPermission](schemas-applicationpermission.md) |  |
| <a id="s-55c7639541"></a>`resource` | no | [ApplicationResource](schemas-applicationresource.md); default="*" |  |

### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1](#s-d2b74a9300) |

### <a id="s-d2b74a9300"></a>`allOf` alternative 1


#### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1 · `oneOf` alternative 1](#s-7f168acadc) |
| 2 | [See `allOf` alternative 1 · `oneOf` alternative 2](#s-ccdb4c072f) |
| 3 | [See `allOf` alternative 1 · `oneOf` alternative 3](#s-ab087fc218) |
| 4 | [See `allOf` alternative 1 · `oneOf` alternative 4](#s-ec47e3d124) |

### <a id="s-7f168acadc"></a>`allOf` alternative 1 · `oneOf` alternative 1

- <a id="s-9d494d85d7"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3664b7b82d"></a>`permission` | yes | const="*" |  |
| <a id="s-2cfc84f7ee"></a>`resource` | no | const="*" |  |

### <a id="s-ccdb4c072f"></a>`allOf` alternative 1 · `oneOf` alternative 2

- <a id="s-3848841b8c"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72430d69a8"></a>`permission` | yes | const="collections:create" |  |
| <a id="s-c2ddb5c9d9"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+)$" |  |

### <a id="s-ab087fc218"></a>`allOf` alternative 1 · `oneOf` alternative 3

- <a id="s-b8b0418ede"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dfcea6cfd5"></a>`permission` | yes | enum=["archives:manage","archives:read","catalog:read","collection-descriptions:manage","collection-tags:manage","collections:delete","provenance:export","provenance:read","retrieval:manage"] |  |
| <a id="s-3dd09897a8"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$" |  |

### <a id="s-ec47e3d124"></a>`allOf` alternative 1 · `oneOf` alternative 4

- <a id="s-1eaae3588d"></a>`required`: `["permission"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b0535ce363"></a>`permission` | yes | enum=["collection-transforms:control","collection-transforms:execute","events:read","events:read_all","keys:manage","quotas:manage"] |  |
| <a id="s-6f3ac360ab"></a>`resource` | no | const="*" |  |

## Maintained corroboration

### Referenced contract elements

- [ApplicationPermission](schemas-applicationpermission.md)
- [ApplicationResource](schemas-applicationresource.md)

## Governing policies

- <a id="pa-bab1265349"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/MutateAppAccessRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 638f68b00b9e16531f633fb81b04206fcfe4916699fb4b3adb55db52dc751d79 -->

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
  "title": "MutateAppAccessRequest",
  "type": "object"
}
```

</details>
