# stove0_operator_contracts.AdmissionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicy:37fbbc74d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b469ad5ed5"></a>
- <a id="s-6c74bb7c21"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e11f6dbc15"></a>`module`: `stove0_operator_contracts`
- <a id="s-85d262fff3"></a>`name`: `AdmissionPolicy`
- <a id="s-027fdcf56d"></a>`unit`: `export`

### Declared structure

- <a id="s-91266566bb"></a>`kind`: `"class"`
- <a id="s-6c8e30fa14"></a>`signature`: `"\"(*, format: Literal['stove0-admission-policy/v1'] = 'stove0-admission-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], selector: stove0_operator_contracts.AllVisibleAdmissionSelector \| stove0_operator_contracts.TaggedAdmissionSelector, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[NonnegativeDecimal, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue] = <factory>, automatic_preview: Literal['accept-ready'] = 'accept-ready') -> None\""`

#### Validated model schema

<a id="s-886f287521"></a>

- <a id="s-845b3374ea"></a>`type`: `"object"`
- <a id="s-a2cf20a7de"></a>`additionalProperties`: `false`
- <a id="s-5a724a63e3"></a>`required`: `["id","revision","selector","recipe_id","recipe_revision","recipe_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eaea3852d7"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready" |  |
| <a id="s-443f9b71cf"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-7830c390f5)) |  |
| <a id="s-9bad2e23cc"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1" |  |
| <a id="s-c56cfe0f78"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4ea2bba8aa"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-e69fcbefed"></a>`recipe_revision` | yes | [NonnegativeDecimal](#s-cc1692615c); ge=1 |  |
| <a id="s-69008c4330"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7e13c20b0c"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-47229d9cc7"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-4c2c6914fa)); ([TaggedAdmissionSelector](#s-af0eb7829b))] |  |

##### Definitions

- [AllVisibleAdmissionSelector](#s-4c2c6914fa)
- [CollectionTag](#s-e223a27a21)
- [JsonValue](#s-7830c390f5)
- [NonnegativeDecimal](#s-cc1692615c)
- [TaggedAdmissionSelector](#s-af0eb7829b)

##### <a id="s-4c2c6914fa"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-38404ec88e"></a>`type`: `"object"`
- <a id="s-8bfcc70703"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-167df1f787"></a>`kind` | no | type="string"; const="all"; default="all" |  |

##### <a id="s-e223a27a21"></a>definition `CollectionTag`

- <a id="s-1620ea55a4"></a>`type`: `"string"`
- <a id="s-a7700501f3"></a>`maxLength`: `65536`
- <a id="s-f970ec8523"></a>`minLength`: `1`
- <a id="s-faa8d66d4a"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-08a475b9ee"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-9a49382214"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-7830c390f5"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-cc1692615c"></a>definition `NonnegativeDecimal`

- <a id="s-be82d2e270"></a>`type`: `"string"`
- <a id="s-ee6bd5bebd"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-af0eb7829b"></a>definition `TaggedAdmissionSelector`

- <a id="s-425d90e344"></a>`type`: `"object"`
- <a id="s-2e6eb15eb2"></a>`additionalProperties`: `false`
- <a id="s-c2e1d2a1f8"></a>`required`: `["required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b6ec672b06"></a>`kind` | no | type="string"; const="tags"; default="tags" |  |
| <a id="s-4da60062b6"></a>`required` | yes | type="array"; items=([CollectionTag](#s-e223a27a21)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

## Maintained corroboration

### Related interface records

- [policy_sha256](stove0-operator-contracts-admissionpolicy-policy-sha256.md)

## Governing policies

- <a id="pa-ce091d4582"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2be9a4781158c5b214cc9704bf2c562d2a42ee77096e8ee08927c416f83dd307 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AllVisibleAdmissionSelector": {
          "additionalProperties": false,
          "properties": {
            "kind": {
              "const": "all",
              "default": "all",
              "type": "string"
            }
          },
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
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "TaggedAdmissionSelector": {
          "additionalProperties": false,
          "properties": {
            "kind": {
              "const": "tags",
              "default": "tags",
              "type": "string"
            },
            "required": {
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
            }
          },
          "required": [
            "required"
          ],
          "type": "object"
        }
      },
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
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "recipe_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "revision": {
          "minimum": 1,
          "type": "integer"
        },
        "selector": {
          "discriminator": {
            "mapping": {
              "all": "#/$defs/AllVisibleAdmissionSelector",
              "tags": "#/$defs/TaggedAdmissionSelector"
            },
            "propertyName": "kind"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/AllVisibleAdmissionSelector"
            },
            {
              "$ref": "#/$defs/TaggedAdmissionSelector"
            }
          ]
        }
      },
      "required": [
        "id",
        "revision",
        "selector",
        "recipe_id",
        "recipe_revision",
        "recipe_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-admission-policy/v1'] = 'stove0-admission-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], selector: stove0_operator_contracts.AllVisibleAdmissionSelector | stove0_operator_contracts.TaggedAdmissionSelector, recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[NonnegativeDecimal, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue] = <factory>, automatic_preview: Literal['accept-ready'] = 'accept-ready') -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPolicy",
  "unit": "export"
}
```

</details>
