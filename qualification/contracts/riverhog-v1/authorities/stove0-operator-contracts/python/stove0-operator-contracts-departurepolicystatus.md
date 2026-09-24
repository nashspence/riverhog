# stove0_operator_contracts.DeparturePolicyStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurepolicystatus:f4d5888fd8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e774f7ecc"></a>
- <a id="s-36d8e53683"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-c0eb06f1b3"></a>`module`: `stove0_operator_contracts`
- <a id="s-0ca3f6db1f"></a>`name`: `DeparturePolicyStatus`
- <a id="s-ca37bd41c3"></a>`unit`: `export`

### Declared structure

- <a id="s-fa319dfd83"></a>`kind`: `"class"`
- <a id="s-219ca79e7f"></a>`signature`: `"\"(*, policy: stove0_operator_contracts.DeparturePolicy, policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['new', 'baseline', 'following', 'reset_required'], source_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, authorization_view_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, through_revision: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:0\|[1-9][0-9]*)$')], updated_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""`

#### Validated model schema

<a id="s-99f305fefc"></a>

- <a id="s-d74637a432"></a>`type`: `"object"`
- <a id="s-f8ad261512"></a>`additionalProperties`: `false`
- <a id="s-359c3df47f"></a>`required`: `["policy","policy_sha256","phase","through_revision","updated_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-457d12fbc0"></a>`authorization_view_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-cdba3b5bd2"></a>`phase` | yes | type="string"; enum=["new","baseline","following","reset_required"] |  |
| <a id="s-01b80342b9"></a>`policy` | yes | [DeparturePolicy](#s-3cd1ff788e) |  |
| <a id="s-a7ee7c0001"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a887f15553"></a>`source_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-0b3bb414ff"></a>`through_revision` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]*)$" |  |
| <a id="s-2407c0ce72"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |

##### Definitions

- [AllVisibleAdmissionSelector](#s-fa62b7ae66)
- [CollectionTag](#s-b6c17d72a3)
- [DeparturePolicy](#s-3cd1ff788e)
- [TaggedAdmissionSelector](#s-bc5bd35e5d)

##### <a id="s-fa62b7ae66"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-1dadc2a101"></a>`type`: `"object"`
- <a id="s-ed29150e10"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4df0e941e4"></a>`kind` | no | type="string"; const="all"; default="all" |  |

##### <a id="s-b6c17d72a3"></a>definition `CollectionTag`

- <a id="s-7aeb910128"></a>`type`: `"string"`
- <a id="s-6a1390ad78"></a>`maxLength`: `65536`
- <a id="s-8b9af03bf9"></a>`minLength`: `1`
- <a id="s-3e344aae0a"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-dc97701c8b"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-631f6903b1"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-3cd1ff788e"></a>definition `DeparturePolicy`

- <a id="s-4ebe19537e"></a>`type`: `"object"`
- <a id="s-dc82f499be"></a>`additionalProperties`: `false`
- <a id="s-53d9998f04"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9026c5499b"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1" |  |
| <a id="s-79d7f94fe5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1f99735fa8"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-6f9e5a1301"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-fa62b7ae66)); ([TaggedAdmissionSelector](#s-bc5bd35e5d))] |  |
| <a id="s-06818736f8"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7436dddc22"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-bc5bd35e5d"></a>definition `TaggedAdmissionSelector`

- <a id="s-df313206ce"></a>`type`: `"object"`
- <a id="s-c57aa40b0e"></a>`additionalProperties`: `false`
- <a id="s-429a896fc8"></a>`required`: `["required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-adaeab8379"></a>`kind` | no | type="string"; const="tags"; default="tags" |  |
| <a id="s-0598309144"></a>`required` | yes | type="array"; items=([CollectionTag](#s-b6c17d72a3)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

## Maintained corroboration

### Related interface records

- [exact_policy](stove0-operator-contracts-departurepolicystatus-exact-policy.md)

## Governing policies

- <a id="pa-474a6427bd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DeparturePolicyStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0decc30b248d75d3076f59f0ffa2575670825b35532f6578a81ea6f4b90cca69 -->

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
          "$ref": "#/$defs/DeparturePolicy"
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        }
      },
      "required": [
        "policy",
        "policy_sha256",
        "phase",
        "through_revision",
        "updated_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, policy: stove0_operator_contracts.DeparturePolicy, policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['new', 'baseline', 'following', 'reset_required'], source_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, authorization_view_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, through_revision: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:0|[1-9][0-9]*)$')], updated_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "DeparturePolicyStatus",
  "unit": "export"
}
```

</details>
