# stove0_operator_contracts.AdmissionCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissioncatalog:95e5135c66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6dcc5be0e9"></a>
- <a id="s-3ab6af11dd"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d3474dcc8d"></a>`module`: `stove0_operator_contracts`
- <a id="s-dc103249f8"></a>`name`: `AdmissionCatalog`
- <a id="s-b029c2dcf2"></a>`unit`: `export`

### Declared structure

- <a id="s-4e0c80bc17"></a>`kind`: `"class"`
- <a id="s-6526b9a9fa"></a>`signature`: `"\"(*, format: Literal['stove0-admissions/v1'] = 'stove0-admissions/v1', policies: Annotated[tuple[stove0_operator_contracts.AdmissionPolicy, ...], MaxLen(max_length=100)] = ()) -> None\""`

#### Validated model schema

<a id="s-e70a4b1460"></a>
- <a id="s-42d5ab1a2b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-844450f375"></a>`format` | no | type="string"; const="stove0-admissions/v1" |  |
| <a id="s-4996621053"></a>`policies` | no | type="array"; maxItems=100; items=(#/$defs/AdmissionPolicy); additional keys=`x-riverhog-extent` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-0d836ab3d0"></a>`AdmissionPolicy` | type="object"; fields=`automatic_preview`, `effective_intent`, `format`, `id`, `recipe_id`, `recipe_revision`, `recipe_sha256`, `required_tags`, `revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-faf360b591"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-0f94759ee8"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionCatalog.canonical_policies](stove0-operator-contracts-admissioncatalog-canonical-policies.md)
- [stove0_operator_contracts.AdmissionCatalog.catalog_sha256](stove0-operator-contracts-admissioncatalog-catalog-sha256.md)

## Governing policies

- <a id="pa-ece43c0ae8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionCatalog`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 401c080d68cad2f5e4adddcbf4198e2a14bb2ee9153dcc5bc587b1f7876ded0b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AdmissionPolicy": {
          "additionalProperties": false,
          "properties": {
            "automatic_preview": {
              "const": "accept-ready",
              "default": "accept-ready",
              "type": "string"
            },
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "format": {
              "const": "stove0-admission-policy/v1",
              "default": "stove0-admission-policy/v1",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "recipe_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "recipe_revision": {
              "minimum": 1,
              "type": "integer"
            },
            "recipe_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "required_tags": {
              "items": {
                "$ref": "#/$defs/CollectionTag"
              },
              "maxItems": 100,
              "minItems": 1,
              "type": "array",
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-exact-classification-admission-predicate"
              }
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
            }
          },
          "required": [
            "id",
            "revision",
            "required_tags",
            "recipe_id",
            "recipe_revision",
            "recipe_sha256"
          ],
          "type": "object"
        },
        "CollectionTag": {
          "maxLength": 65536,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-collection-tag"
          },
          "x-unicode-normalization": "NFC"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-admissions/v1",
          "default": "stove0-admissions/v1",
          "type": "string"
        },
        "policies": {
          "default": [],
          "items": {
            "$ref": "#/$defs/AdmissionPolicy"
          },
          "maxItems": 100,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-deployment-admission-catalog"
          }
        }
      },
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-admissions/v1'] = 'stove0-admissions/v1', policies: Annotated[tuple[stove0_operator_contracts.AdmissionPolicy, ...], MaxLen(max_length=100)] = ()) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionCatalog",
  "unit": "export"
}
```
