# riverhog_application_access.ApplicationAccessGrantSet

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationac-d1ab2b2bec:6297aef642 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-a4e482d604"></a>`type`: `"array"`
- <a id="s-62df42db3d"></a>`items`: [ApplicationAccessGrant](#s-50674c05ad)
- <a id="s-5e709c6a89"></a>`minItems`: `1`
- <a id="s-3eddd136fb"></a>`uniqueItems`: `true`

##### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-a25051a2b0"></a>1 | contains=(type="object"; properties={permission: (const="*")}; required=["permission"]) | maxItems=1; x-riverhog-extent={"policy":"contract_max","reason":"wildcard-access-grant-is-exclusive"} | no additional constraint |

##### Definitions

- [ApplicationAccessGrant](#s-50674c05ad)
- [ApplicationPermission](#s-af1e2efa80)
- [ApplicationResource](#s-b149116dc2)

##### <a id="s-50674c05ad"></a>definition `ApplicationAccessGrant`

- <a id="s-27c3729410"></a>`type`: `"object"`
- <a id="s-1be680e47c"></a>`additionalProperties`: `false`
- <a id="s-59803e54c3"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2057073be0"></a>`permission` | yes | [ApplicationPermission](#s-af1e2efa80) |  |
| <a id="s-f1eed063dd"></a>`resource` | no | [ApplicationResource](#s-b149116dc2); default="*" |  |

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| 1 | [See definition `ApplicationAccessGrant` · `allOf` alternative 1](#s-8f5a7cd5d4) |

##### <a id="s-af1e2efa80"></a>definition `ApplicationPermission`

- <a id="s-dc3dc429b0"></a>`type`: `"string"`
- <a id="s-595a73587e"></a>`enum`: `["*","catalog:read","retrieval:manage","collections:create","collection-descriptions:manage","collection-transforms:control","collection-transforms:execute","collection-tags:manage","collections:delete","archives:read","archives:manage","keys:manage","quotas:manage","events:read","events:read_all","provenance:read","provenance:export"]`

##### <a id="s-b149116dc2"></a>definition `ApplicationResource`

- <a id="s-5fc8dace7f"></a>`type`: `"string"`
- <a id="s-0f8b1f7c74"></a>`pattern`: `"^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$"`

##### <a id="s-8f5a7cd5d4"></a>definition `ApplicationAccessGrant` · `allOf` alternative 1


###### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 1](#s-dc9658a55b) |
| 2 | [See definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 2](#s-39646dacae) |
| 3 | [See definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 3](#s-3e48830d48) |
| 4 | [See definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 4](#s-869b61ca1f) |

##### <a id="s-dc9658a55b"></a>definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 1

- <a id="s-4cebb0bd87"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c68c860cd"></a>`permission` | yes | const="*" |  |
| <a id="s-c136bdcc5f"></a>`resource` | no | const="*" |  |

##### <a id="s-39646dacae"></a>definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 2

- <a id="s-275a011c79"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-47586effba"></a>`permission` | yes | const="collections:create" |  |
| <a id="s-b9c1f3757a"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+)$" |  |

##### <a id="s-3e48830d48"></a>definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 3

- <a id="s-427cced805"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-43d0ada6b7"></a>`permission` | yes | enum=["archives:manage","archives:read","catalog:read","collection-descriptions:manage","collection-tags:manage","collections:delete","provenance:export","provenance:read","retrieval:manage"] |  |
| <a id="s-6f1844bbc1"></a>`resource` | no | type="string"; pattern="^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$" |  |

##### <a id="s-869b61ca1f"></a>definition `ApplicationAccessGrant` · `allOf` alternative 1 · `oneOf` alternative 4

- <a id="s-bfb5fd30e2"></a>`required`: `["permission"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-18903ed3ee"></a>`permission` | yes | enum=["collection-transforms:control","collection-transforms:execute","events:read","events:read_all","keys:manage","quotas:manage"] |  |
| <a id="s-48e34a6fb6"></a>`resource` | no | const="*" |  |

## Maintained corroboration

### Related interface records

- [validate_set](riverhog-application-access-applicationaccessgrantset-validate-set.md)

## Governing policies

- <a id="pa-c98301a488"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccessGrantSet`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
