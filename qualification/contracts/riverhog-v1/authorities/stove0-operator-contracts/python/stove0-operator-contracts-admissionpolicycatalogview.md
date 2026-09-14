# stove0_operator_contracts.AdmissionPolicyCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicycatalogview:02bee760ab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61e469f9ee"></a>
- <a id="s-8675565fed"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-075b595da8"></a>`module`: `stove0_operator_contracts`
- <a id="s-d246c344a0"></a>`name`: `AdmissionPolicyCatalogView`
- <a id="s-46a485b2e3"></a>`unit`: `export`

### Declared structure

- <a id="s-9f593ba909"></a>`kind`: `"class"`
- <a id="s-d68cb15559"></a>`signature`: `"\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policies: tuple[stove0_operator_contracts.AdmissionPolicyStatus, ...]) -> None\""`

#### Validated model schema

<a id="s-ec615fdcad"></a>
- <a id="s-bd5dfbbf77"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-80006ec8ce"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f1bfd3585b"></a>`policies` | yes | type="array"; items=(#/$defs/AdmissionPolicyStatus) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-13abd4b696"></a>`AdmissionPolicy` | type="object"; fields=`automatic_preview`, `effective_intent`, `format`, `id`, `recipe_id`, `recipe_revision`, `recipe_sha256`, `required_tags`, `revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-56b276ba28"></a>`AdmissionPolicyStatus` | type="object"; fields=`authorization_view_identity`, `baseline_mode`, `phase`, `policy`, `policy_sha256`, `source_identity`, `through_revision`, `updated_at`; additional keys=`additionalProperties`, `required` |
| <a id="s-cb5907c3dd"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-b7b9e89314"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-4deee81d84"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicyCatalogView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a69a139639800be2b7dcd38831eb7348c5ad02b04782a8a7fe2b9da7b0705dc9 -->

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
        "AdmissionPolicyStatus": {
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
              "default": null
            },
            "baseline_mode": {
              "enum": [
                "observe",
                "backfill"
              ],
              "type": "string"
            },
            "phase": {
              "enum": [
                "new",
                "baseline",
                "following",
                "reset_required"
              ],
              "type": "string"
            },
            "policy": {
              "$ref": "#/$defs/AdmissionPolicy"
            },
            "policy_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "default": null
            },
            "through_revision": {
              "pattern": "^(?:0|[1-9][0-9]*)$",
              "type": "string"
            },
            "updated_at": {
              "maxLength": 40,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "policy",
            "policy_sha256",
            "phase",
            "baseline_mode",
            "through_revision",
            "updated_at"
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
        "catalog_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "policies": {
          "items": {
            "$ref": "#/$defs/AdmissionPolicyStatus"
          },
          "type": "array"
        }
      },
      "required": [
        "catalog_sha256",
        "policies"
      ],
      "type": "object"
    },
    "signature": "\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policies: tuple[stove0_operator_contracts.AdmissionPolicyStatus, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPolicyCatalogView",
  "unit": "export"
}
```
