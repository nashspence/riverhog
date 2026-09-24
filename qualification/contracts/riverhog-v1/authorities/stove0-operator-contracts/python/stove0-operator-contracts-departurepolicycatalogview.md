# stove0_operator_contracts.DeparturePolicyCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurepolicycatalogview:03891d3652 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-21d813289e"></a>
- <a id="s-aa5fb7ec14"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-0b36c979bb"></a>`module`: `stove0_operator_contracts`
- <a id="s-7a36448284"></a>`name`: `DeparturePolicyCatalogView`
- <a id="s-d92327bebb"></a>`unit`: `export`

### Declared structure

- <a id="s-88e605a16a"></a>`kind`: `"class"`
- <a id="s-dc24bf53c0"></a>`signature`: `"\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policies: tuple[stove0_operator_contracts.DeparturePolicyStatus, ...]) -> None\""`

#### Validated model schema

<a id="s-ffa56929a4"></a>

- <a id="s-bf70f67a95"></a>`type`: `"object"`
- <a id="s-16b1ae3fa7"></a>`additionalProperties`: `false`
- <a id="s-d56aa94b7b"></a>`required`: `["catalog_sha256","policies"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-023f1dffc3"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3bbddd39b1"></a>`policies` | yes | type="array"; items=([DeparturePolicyStatus](#s-4e8fffa24f)) |  |

##### Definitions

- [AllVisibleAdmissionSelector](#s-aa021513db)
- [CollectionTag](#s-8c5b5db184)
- [DeparturePolicy](#s-0c9f41590f)
- [DeparturePolicyStatus](#s-4e8fffa24f)
- [TaggedAdmissionSelector](#s-98e6e4afea)

##### <a id="s-aa021513db"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-1248b1c572"></a>`type`: `"object"`
- <a id="s-4e23061a44"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cd6588be36"></a>`kind` | no | type="string"; const="all"; default="all" |  |

##### <a id="s-8c5b5db184"></a>definition `CollectionTag`

- <a id="s-3fee5c8ff5"></a>`type`: `"string"`
- <a id="s-0e7f9facde"></a>`maxLength`: `65536`
- <a id="s-271281bc40"></a>`minLength`: `1`
- <a id="s-41e21b76f4"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-5cd8e29636"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-c08c118338"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-0c9f41590f"></a>definition `DeparturePolicy`

- <a id="s-2d48044416"></a>`type`: `"object"`
- <a id="s-947c368b15"></a>`additionalProperties`: `false`
- <a id="s-fe7b46bcc2"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60ae8b860a"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1" |  |
| <a id="s-ea2430bc4f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-667053ca63"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-d2c63bc2b0"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-aa021513db)); ([TaggedAdmissionSelector](#s-98e6e4afea))] |  |
| <a id="s-eecc127d2e"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2de3e0689a"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-4e8fffa24f"></a>definition `DeparturePolicyStatus`

- <a id="s-fe7ae1404c"></a>`type`: `"object"`
- <a id="s-c6bd094b82"></a>`additionalProperties`: `false`
- <a id="s-f035ba4417"></a>`required`: `["policy","policy_sha256","phase","through_revision","updated_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a8f2490d7"></a>`authorization_view_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-79e1e829a1"></a>`phase` | yes | type="string"; enum=["new","baseline","following","reset_required"] |  |
| <a id="s-fc8ebe3610"></a>`policy` | yes | [DeparturePolicy](#s-0c9f41590f) |  |
| <a id="s-ed49b5427e"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f345de41ff"></a>`source_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-4267ae3068"></a>`through_revision` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]*)$" |  |
| <a id="s-dd63e94150"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |

##### <a id="s-98e6e4afea"></a>definition `TaggedAdmissionSelector`

- <a id="s-23ab2a1e13"></a>`type`: `"object"`
- <a id="s-ec8a08865e"></a>`additionalProperties`: `false`
- <a id="s-5a34e997f1"></a>`required`: `["required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ada32d2c6c"></a>`kind` | no | type="string"; const="tags"; default="tags" |  |
| <a id="s-9f51155b8a"></a>`required` | yes | type="array"; items=([CollectionTag](#s-8c5b5db184)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

## Governing policies

- <a id="pa-49dc0d4f87"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DeparturePolicyCatalogView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 967c7c72a037b3449fa0a660b7f2ced0ee3a2ee8a9535957c5747ce470a01495 -->

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
        "DeparturePolicyStatus": {
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
        "catalog_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "policies": {
          "items": {
            "$ref": "#/$defs/DeparturePolicyStatus"
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
    "signature": "\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], policies: tuple[stove0_operator_contracts.DeparturePolicyStatus, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "DeparturePolicyCatalogView",
  "unit": "export"
}
```

</details>
