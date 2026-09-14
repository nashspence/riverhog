# riverhog_application_access.ApplicationAccessGrantSet

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationac-d1ab2b2bec:6297aef642 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20a2359614"></a>
- <a id="s-356f252e2b"></a>`distribution`: `riverhog-application-access`
- <a id="s-52e1c566f6"></a>`module`: `riverhog_application_access`
- <a id="s-3b142ea758"></a>`name`: `ApplicationAccessGrantSet`
- <a id="s-0d1940f3a2"></a>`unit`: `export`

### Declared structure

- <a id="s-23691e3032"></a>`kind`: `"class"`
- <a id="s-70601b24ee"></a>`signature`: `"\"(root: 'RootModelRootType' = PydanticUndefined) -> None\""`

#### Validated model schema

<a id="s-4421aea33c"></a>
- <a id="s-a4e482d604"></a>`type`: array

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-50674c05ad"></a>`ApplicationAccessGrant` | type="object"; fields=`permission`, `resource`; allOf=oneOf=fields=`permission`, `resource`; additional keys=`required` \| fields=`permission`, `resource`; additional keys=`required` \| fields=`permission`, `resource`; additional keys=`required` \| fields=`permission`, `resource`; additional keys=`required`; additional keys=`additionalProperties`, `required` |
| <a id="s-af1e2efa80"></a>`ApplicationPermission` | type="string"; enum=["*","catalog:read","retrieval:manage","collections:create","collection-descriptions:manage","collection-transforms:control","collection-transforms:execute","collection-tags:manage","collections:delete","archives:read","archives:manage","keys:manage","quotas:manage","events:read","events:read_all","provenance:read","provenance:export"] |
| <a id="s-b149116dc2"></a>`ApplicationResource` | type="string"; pattern="^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$" |

## Maintained corroboration

### Related interface records

- [validate_set](riverhog-application-access-applicationaccessgrantset-validate-set.md)

## Governing policies

- <a id="pa-c98301a488"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrantSet`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6c340fc5015cb3a969ef453f95ca210ff51f7e0755d7018ac298b661a1bc809 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ApplicationAccessGrant": {
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
      "allOf": [
        {
          "if": {
            "contains": {
              "properties": {
                "permission": {
                  "const": "*"
                }
              },
              "required": [
                "permission"
              ],
              "type": "object"
            }
          },
          "then": {
            "maxItems": 1,
            "x-riverhog-extent": {
              "policy": "contract_max",
              "reason": "wildcard-access-grant-is-exclusive"
            }
          }
        }
      ],
      "items": {
        "$ref": "#/$defs/ApplicationAccessGrant"
      },
      "minItems": 1,
      "type": "array",
      "uniqueItems": true
    },
    "signature": "\"(root: 'RootModelRootType' = PydanticUndefined) -> None\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationAccessGrantSet",
  "unit": "export"
}
```
