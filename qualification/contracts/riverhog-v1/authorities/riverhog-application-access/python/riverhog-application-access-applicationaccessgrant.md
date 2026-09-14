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
- <a id="s-f90cbefa14"></a>`title`: ApplicationAccessGrant
- <a id="s-241714f192"></a>`description`: One canonical public application-access request or response grant.
- <a id="s-2bc5cade12"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35340f1f8d"></a>`permission` | yes | #/$defs/ApplicationPermission |  |
| <a id="s-d717c2ef66"></a>`resource` | no | $ref="#/$defs/ApplicationResource" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-95da86e4fe"></a>`ApplicationPermission` | type="string"; enum=["*","catalog:read","retrieval:manage","collections:create","collection-descriptions:manage","collection-transforms:control","collection-transforms:execute","collection-tags:manage","collections:delete","archives:read","archives:manage","keys:manage","quotas:manage","events:read","events:read_all","provenance:read","provenance:export"] |
| <a id="s-1bf3d3f588"></a>`ApplicationResource` | type="string"; pattern="^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$" |

## Maintained corroboration

### Related interface records

- [riverhog_application_access.ApplicationAccessGrant.as_access](riverhog-application-access-applicationaccessgrant-as-access.md)
- [riverhog_application_access.ApplicationAccessGrant.validate_relationship](riverhog-application-access-applicationaccessgrant-validate-relationship.md)

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

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41115c446c549121cfa8bb627234156538a8cdb9ba411a5a6553ce5c32e56fda -->

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
      "description": "One canonical public application-access request or response grant.",
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
      "title": "ApplicationAccessGrant",
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
