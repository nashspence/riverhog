# riverhog_application_access.ApplicationAccessGrant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationaccessgrant:fbd0a6fd2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-487340b284"></a>
- <a id="s-e4911eddea"></a>`distribution`: `riverhog-application-access`
- <a id="s-2403018936"></a>`module`: `riverhog_application_access`
- <a id="s-f25a65db4c"></a>`name`: `ApplicationAccessGrant`
- <a id="s-e84cd86f89"></a>`unit`: `export`

### Declared structure

- <a id="s-cd3899efb1"></a>`kind`: `"class"`
- <a id="s-6e15bb70eb"></a>`signature`: `"\"(*, permission: ApplicationPermission, resource: ApplicationResource = '*') -> None\""`

#### Validated model schema

<a id="s-2f97f3665f"></a>

- <a id="s-2bc5cade12"></a>`type`: `"object"`
- <a id="s-e87c9f73b7"></a>`additionalProperties`: `false`
- <a id="s-e7a42c1f7e"></a>`required`: `["permission"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35340f1f8d"></a>`permission` | yes | [ApplicationPermission](#s-95da86e4fe) |  |
| <a id="s-d717c2ef66"></a>`resource` | no | [ApplicationResource](#s-1bf3d3f588); default="*" |  |

##### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1](#s-28f68c3e3c) |

##### Definitions

- [ApplicationPermission](#s-95da86e4fe)
- [ApplicationResource](#s-1bf3d3f588)

##### <a id="s-28f68c3e3c"></a>`allOf` alternative 1


###### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `allOf` alternative 1 · `oneOf` alternative 1](#s-53a64992c8) |
| 2 | [See `allOf` alternative 1 · `oneOf` alternative 2](#s-b108ef462f) |
| 3 | [See `allOf` alternative 1 · `oneOf` alternative 3](#s-2a64f7dca1) |
| 4 | [See `allOf` alternative 1 · `oneOf` alternative 4](#s-e79aeca70d) |

##### <a id="s-95da86e4fe"></a>definition `ApplicationPermission`

- <a id="s-f3bef043e6"></a>`type`: `"string"`
- <a id="s-233b95f6a5"></a>`enum`: `["*","catalog:read","retrieval:manage","collections:create","collection-descriptions:manage","collection-transforms:control","collection-transforms:execute","collection-tags:manage","collections:delete","archives:read","archives:manage","keys:manage","quotas:manage","events:read","events:read_all","provenance:read","provenance:export"]`

##### <a id="s-1bf3d3f588"></a>definition `ApplicationResource`

- <a id="s-c0fcacb434"></a>`type`: `"string"`
- <a id="s-a5cb46c823"></a>`pattern`: `"^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$"`

##### <a id="s-53a64992c8"></a>`allOf` alternative 1 · `oneOf` alternative 1

- <a id="s-f9a03274be"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-91e42ab37c"></a>`permission` | yes | const="*" |  |
| <a id="s-a21cbeb555"></a>`resource` | no | const="*" |  |

##### <a id="s-b108ef462f"></a>`allOf` alternative 1 · `oneOf` alternative 2

- <a id="s-c41dfa12d0"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5c2c14e3a"></a>`permission` | yes | const="collections:create" |  |
| <a id="s-1aa433d478"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+)$" |  |

##### <a id="s-2a64f7dca1"></a>`allOf` alternative 1 · `oneOf` alternative 3

- <a id="s-a13b889bca"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-26226fddd5"></a>`permission` | yes | enum=["archives:manage","archives:read","catalog:read","collection-descriptions:manage","collection-tags:manage","collections:delete","provenance:export","provenance:read","retrieval:manage"] |  |
| <a id="s-48c37fb9d1"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$" |  |

##### <a id="s-e79aeca70d"></a>`allOf` alternative 1 · `oneOf` alternative 4

- <a id="s-2b7345e034"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-433202c11e"></a>`permission` | yes | enum=["collection-transforms:control","collection-transforms:execute","events:read","events:read_all","keys:manage","quotas:manage"] |  |
| <a id="s-cd48ffb6ab"></a>`resource` | no | const="*" |  |

## Maintained corroboration

### Related interface records

- [as_access](riverhog-application-access-applicationaccessgrant-as-access.md)
- [validate_relationship](riverhog-application-access-applicationaccessgrant-validate-relationship.md)

## Governing policies

- <a id="pa-48133bba5a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrant`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d824cf0c37fde02c090eabd05e06ada64c2a3946bfa09e5bbb31e0611439719 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ApplicationPermission": {
          "enum": [
            "*",
            "catalog:read",
            "retrieval:manage",
            "collections:create",
            "collection-descriptions:manage",
            "collection-transforms:control",
            "collection-transforms:execute",
            "collection-tags:manage",
            "collections:delete",
            "archives:read",
            "archives:manage",
            "keys:manage",
            "quotas:manage",
            "events:read",
            "events:read_all",
            "provenance:read",
            "provenance:export"
          ],
          "type": "string"
        },
        "ApplicationResource": {
          "pattern": "^(?:\\*|tag:.+|collection:[1-9][0-9]*)$",
          "type": "string"
        }
      },
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
          "$ref": "#/$defs/ApplicationPermission"
        },
        "resource": {
          "$ref": "#/$defs/ApplicationResource",
          "default": "*"
        }
      },
      "required": [
        "permission"
      ],
      "type": "object"
    },
    "signature": "\"(*, permission: ApplicationPermission, resource: ApplicationResource = '*') -> None\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationAccessGrant",
  "unit": "export"
}
```

</details>
