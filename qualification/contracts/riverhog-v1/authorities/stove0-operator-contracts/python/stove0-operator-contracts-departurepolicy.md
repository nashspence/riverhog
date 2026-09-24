# stove0_operator_contracts.DeparturePolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurepolicy:819024d82f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-27a7913fba"></a>
- <a id="s-e2d2003642"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e144cc7bfd"></a>`module`: `stove0_operator_contracts`
- <a id="s-4ba964dfbb"></a>`name`: `DeparturePolicy`
- <a id="s-6a5f8e9ca2"></a>`unit`: `export`

### Declared structure

- <a id="s-22e9329a5b"></a>`kind`: `"class"`
- <a id="s-834b8e5177"></a>`signature`: `"\"(*, format: Literal['stove0-departure-policy/v1'] = 'stove0-departure-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], selector: stove0_operator_contracts.AllVisibleAdmissionSelector \| stove0_operator_contracts.TaggedAdmissionSelector, target_registration_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-408bcbff8d"></a>

- <a id="s-692dc40f6e"></a>`type`: `"object"`
- <a id="s-e049ca70b3"></a>`additionalProperties`: `false`
- <a id="s-60f5dae9b6"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9a10ebbcb9"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1" |  |
| <a id="s-2a59d6b588"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-9282d6e3a7"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-660b2f4115"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-cf710926cc)); ([TaggedAdmissionSelector](#s-3a11b67359))] |  |
| <a id="s-e76c21e1b4"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ca85604c44"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### Definitions

- [AllVisibleAdmissionSelector](#s-cf710926cc)
- [CollectionTag](#s-713d10cc37)
- [TaggedAdmissionSelector](#s-3a11b67359)

##### <a id="s-cf710926cc"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-50add9b954"></a>`type`: `"object"`
- <a id="s-a0fa02f398"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a4a7cbd6f7"></a>`kind` | no | type="string"; const="all"; default="all" |  |

##### <a id="s-713d10cc37"></a>definition `CollectionTag`

- <a id="s-b50b00490b"></a>`type`: `"string"`
- <a id="s-f0de6f3d26"></a>`maxLength`: `65536`
- <a id="s-48da2da522"></a>`minLength`: `1`
- <a id="s-6baa68475d"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-2d57f22ecc"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-431f742098"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-3a11b67359"></a>definition `TaggedAdmissionSelector`

- <a id="s-2f6c42b2fe"></a>`type`: `"object"`
- <a id="s-2dae0ab6ca"></a>`additionalProperties`: `false`
- <a id="s-cc2b100218"></a>`required`: `["required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5d24346940"></a>`kind` | no | type="string"; const="tags"; default="tags" |  |
| <a id="s-3773389ed6"></a>`required` | yes | type="array"; items=([CollectionTag](#s-713d10cc37)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

## Maintained corroboration

### Related interface records

- [policy_sha256](stove0-operator-contracts-departurepolicy-policy-sha256.md)

## Governing policies

- <a id="pa-b8480a2094"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DeparturePolicy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4dcff6ac3293f81cf403115b176ccab0cb7196629e0a91fb4550b3ef602d33ec -->

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
        "format": {
          "const": "stove0-departure-policy/v1",
          "default": "stove0-departure-policy/v1",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
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
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "target_registration_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "selector",
        "target_registration_id",
        "target_identity"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-departure-policy/v1'] = 'stove0-departure-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], selector: stove0_operator_contracts.AllVisibleAdmissionSelector | stove0_operator_contracts.TaggedAdmissionSelector, target_registration_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "DeparturePolicy",
  "unit": "export"
}
```

</details>
