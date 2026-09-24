# stove0_operator_contracts.DepartureCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurecatalog:a48f6558c3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-827c7b178e"></a>
- <a id="s-7aa37111f7"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-26f3346274"></a>`module`: `stove0_operator_contracts`
- <a id="s-612ed958d6"></a>`name`: `DepartureCatalog`
- <a id="s-086ac5a3ae"></a>`unit`: `export`

### Declared structure

- <a id="s-81444a2401"></a>`kind`: `"class"`
- <a id="s-9fc781db57"></a>`signature`: `"\"(*, format: Literal['stove0-departures/v1'] = 'stove0-departures/v1', policies: Annotated[tuple[stove0_operator_contracts.DeparturePolicy, ...], MaxLen(max_length=100)] = ()) -> None\""`

#### Validated model schema

<a id="s-e0ad7d59a9"></a>

- <a id="s-2ee37e7f8f"></a>`type`: `"object"`
- <a id="s-09c40b05f3"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-98bc2c0971"></a>`format` | no | type="string"; const="stove0-departures/v1"; default="stove0-departures/v1" |  |
| <a id="s-f71bf306e0"></a>`policies` | no | type="array"; default=[]; items=([DeparturePolicy](#s-2d906cccd5)); maxItems=100; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-departure-catalog"} |  |

##### Definitions

- [AllVisibleAdmissionSelector](#s-85b14ec8f0)
- [CollectionTag](#s-7b2172737a)
- [DeparturePolicy](#s-2d906cccd5)
- [TaggedAdmissionSelector](#s-e5bffe2fae)

##### <a id="s-85b14ec8f0"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-d1f956f196"></a>`type`: `"object"`
- <a id="s-1e78deb116"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-81e425ce77"></a>`kind` | no | type="string"; const="all"; default="all" |  |

##### <a id="s-7b2172737a"></a>definition `CollectionTag`

- <a id="s-0e020d52a5"></a>`type`: `"string"`
- <a id="s-1a3f1a0003"></a>`maxLength`: `65536`
- <a id="s-a076c57c60"></a>`minLength`: `1`
- <a id="s-fc0e057789"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-5a7bbca011"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-6a70578910"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-2d906cccd5"></a>definition `DeparturePolicy`

- <a id="s-eac7a3e759"></a>`type`: `"object"`
- <a id="s-ebfdaf6252"></a>`additionalProperties`: `false`
- <a id="s-c6a3525fde"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-70c2cbf330"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1" |  |
| <a id="s-33353e8100"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-24533be00b"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-74dea725e9"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-85b14ec8f0)); ([TaggedAdmissionSelector](#s-e5bffe2fae))] |  |
| <a id="s-63969ffef1"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-140eb722b6"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-e5bffe2fae"></a>definition `TaggedAdmissionSelector`

- <a id="s-7b035c82ec"></a>`type`: `"object"`
- <a id="s-1fa88330ac"></a>`additionalProperties`: `false`
- <a id="s-9415e942d8"></a>`required`: `["required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ad5d5f58f"></a>`kind` | no | type="string"; const="tags"; default="tags" |  |
| <a id="s-a63b259afa"></a>`required` | yes | type="array"; items=([CollectionTag](#s-7b2172737a)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

## Maintained corroboration

### Related interface records

- [catalog_sha256](stove0-operator-contracts-departurecatalog-catalog-sha256.md)
- [canonical_policies](stove0-operator-contracts-departurecatalog-canonical-policies.md)

## Governing policies

- <a id="pa-af3006bd52"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DepartureCatalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42d8698708030fac058b1a0049c908e6312aae18e3399b77892398017ca9d0e9 -->

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
        "DeparturePolicy": {
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
          "const": "stove0-departures/v1",
          "default": "stove0-departures/v1",
          "type": "string"
        },
        "policies": {
          "default": [],
          "items": {
            "$ref": "#/$defs/DeparturePolicy"
          },
          "maxItems": 100,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-deployment-departure-catalog"
          }
        }
      },
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-departures/v1'] = 'stove0-departures/v1', policies: Annotated[tuple[stove0_operator_contracts.DeparturePolicy, ...], MaxLen(max_length=100)] = ()) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "DepartureCatalog",
  "unit": "export"
}
```

</details>
