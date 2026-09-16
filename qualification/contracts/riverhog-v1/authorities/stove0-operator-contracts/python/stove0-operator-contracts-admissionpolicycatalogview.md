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

- <a id="s-bd5dfbbf77"></a>`type`: `"object"`
- <a id="s-6d0703c64a"></a>`additionalProperties`: `false`
- <a id="s-a1ed900ee8"></a>`required`: `["catalog_sha256","policies"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-80006ec8ce"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f1bfd3585b"></a>`policies` | yes | type="array"; items=([AdmissionPolicyStatus](#s-56b276ba28)) |  |

##### Definitions

- [AdmissionPolicy](#s-13abd4b696)
- [AdmissionPolicyStatus](#s-56b276ba28)
- [CollectionTag](#s-cb5907c3dd)
- [JsonValue](#s-b7b9e89314)

##### <a id="s-13abd4b696"></a>definition `AdmissionPolicy`

- <a id="s-131899fc44"></a>`type`: `"object"`
- <a id="s-4c83a62cf3"></a>`additionalProperties`: `false`
- <a id="s-6846338fd8"></a>`required`: `["id","revision","required_tags","recipe_id","recipe_revision","recipe_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-23c64f28b9"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready" |  |
| <a id="s-f91c8fef1b"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-b7b9e89314)) |  |
| <a id="s-e54042d6a1"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1" |  |
| <a id="s-97e9aff8d5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b4afc54715"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-28d24257e5"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-0951c23af9"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c6a20ca01c"></a>`required_tags` | yes | type="array"; items=([CollectionTag](#s-cb5907c3dd)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |
| <a id="s-4163c2055a"></a>`revision` | yes | type="integer"; minimum=1 |  |

##### <a id="s-56b276ba28"></a>definition `AdmissionPolicyStatus`

- <a id="s-fb72690b89"></a>`type`: `"object"`
- <a id="s-d63152239e"></a>`additionalProperties`: `false`
- <a id="s-45e3b924a2"></a>`required`: `["policy","policy_sha256","phase","baseline_mode","through_revision","updated_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1247d80eca"></a>`authorization_view_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-924ff01e67"></a>`baseline_mode` | yes | type="string"; enum=["observe","backfill"] |  |
| <a id="s-1769ebc9a5"></a>`phase` | yes | type="string"; enum=["new","baseline","following","reset_required"] |  |
| <a id="s-da17baa0db"></a>`policy` | yes | [AdmissionPolicy](#s-13abd4b696) |  |
| <a id="s-52d72fd271"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ca0cc0b765"></a>`source_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-3656dff895"></a>`through_revision` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]*)$" |  |
| <a id="s-491d13cbc7"></a>`updated_at` | yes | type="string"; maxLength=40; minLength=1 |  |

##### <a id="s-cb5907c3dd"></a>definition `CollectionTag`

- <a id="s-7a6a3c76f4"></a>`type`: `"string"`
- <a id="s-4dce7c49cf"></a>`maxLength`: `65536`
- <a id="s-3ed3d2c5d3"></a>`minLength`: `1`
- <a id="s-f07af3ef6f"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-670d4923fb"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-a52e02cc72"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-b7b9e89314"></a>definition `JsonValue`

- Accepts: any JSON value.

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

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
