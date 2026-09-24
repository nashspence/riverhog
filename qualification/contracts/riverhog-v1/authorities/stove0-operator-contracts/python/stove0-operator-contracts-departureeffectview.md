# stove0_operator_contracts.DepartureEffectView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departureeffectview:ea5aa6a5a4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf4917e964"></a>
- <a id="s-6f89c9f37e"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d6fa809445"></a>`module`: `stove0_operator_contracts`
- <a id="s-833d2ae95b"></a>`name`: `DepartureEffectView`
- <a id="s-c7faaaa3dd"></a>`unit`: `export`

### Declared structure

- <a id="s-dd2e595d07"></a>`kind`: `"class"`
- <a id="s-c0a1ec59e4"></a>`signature`: `"\"(*, intent: stove0_target_protocol.departure.DepartureEffectIntent, state: Literal['pending', 'complete'], receipt: stove0_target_protocol.departure.DepartureEffectReceipt \| None = None, attempt_count: Annotated[int, Ge(ge=0)], next_attempt_at: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]] = None, failure: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, created_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], updated_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""`

#### Validated model schema

<a id="s-d5e1930cf6"></a>

- <a id="s-2ed2f0e3a5"></a>`type`: `"object"`
- <a id="s-606850245a"></a>`additionalProperties`: `false`
- <a id="s-6c208ae95f"></a>`required`: `["intent","state","attempt_count","created_at","updated_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee65ca9b0d"></a>`attempt_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-6e877a62c8"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-9140ddb6a4"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-d8f665adea"></a>`intent` | yes | [DepartureEffectIntent](#s-d544e0cab0) |  |
| <a id="s-9aaa42cf9b"></a>`next_attempt_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-d560eedf5a"></a>`receipt` | no | anyOf=[([DepartureEffectReceipt](#s-8479432c3b)); (type="null")]; default=null |  |
| <a id="s-a7b1700f6c"></a>`state` | yes | type="string"; enum=["pending","complete"] |  |
| <a id="s-7570a32985"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |

##### Definitions

- [CatalogSyncDescriptor](#s-c57a17a1d0)
- [CollectionDescription](#s-ee78e1c8df)
- [CollectionId](#s-9a6f16433b)
- [DepartureEffectIntent](#s-d544e0cab0)
- [DepartureEffectReceipt](#s-8479432c3b)
- [JsonValue](#s-cb6a47750c)

##### <a id="s-c57a17a1d0"></a>definition `CatalogSyncDescriptor`

- <a id="s-e492af4419"></a>`type`: `"object"`
- <a id="s-8303b7e010"></a>`additionalProperties`: `false`
- <a id="s-7529f42d98"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2b2ee939e"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b45b4b9f2"></a>`collection_id` | yes | [CollectionId](#s-9a6f16433b) |  |
| <a id="s-d80071d53c"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9fc259d764"></a>`description` | yes | anyOf=[([CollectionDescription](#s-ee78e1c8df)); (type="null")] |  |
| <a id="s-6759a0f431"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-01feb64c52"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-ff4783907e"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-db0c82320d"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-d960e3da19"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-ee78e1c8df"></a>definition `CollectionDescription`

- <a id="s-8fae36c51d"></a>`type`: `"string"`
- <a id="s-642e810c32"></a>`maxLength`: `32768`
- <a id="s-b73cddbebb"></a>`minLength`: `1`
- <a id="s-4348d41e52"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-1b04211bc3"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-3978fb5c96"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-9a6f16433b"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-e986560cc0"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-b1f6800aa5"></a>2 | not=(const="0") |

##### <a id="s-d544e0cab0"></a>definition `DepartureEffectIntent`

- <a id="s-7c69f77ad1"></a>`type`: `"object"`
- <a id="s-932bbd9a40"></a>`additionalProperties`: `false`
- <a id="s-a79eef748d"></a>`required`: `["policy_id","policy_revision","policy_sha256","target_registration_id","target_identity","source_identity","authorization_view_identity","last_collection","departure_cause","departure_revision","departure_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a128ef2f72"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a472fb9bfd"></a>`departure_cause` | yes | type="string"; enum=["collection_deleted","visibility_lost"] |  |
| <a id="s-1746dfed8a"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5738d4e518"></a>`departure_revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-8738dbaaf6"></a>`format` | no | type="string"; const="stove0-departure-effect-intent/v1"; default="stove0-departure-effect-intent/v1" |  |
| <a id="s-29b319654d"></a>`last_collection` | yes | [CatalogSyncDescriptor](#s-c57a17a1d0) |  |
| <a id="s-5e6809408e"></a>`policy_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-86d02904ad"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-b225fb70ce"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8e1bd1aab"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cfa887326a"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0deefb86b3"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-8479432c3b"></a>definition `DepartureEffectReceipt`

- <a id="s-d4a9107d7a"></a>`type`: `"object"`
- <a id="s-3225b45efa"></a>`additionalProperties`: `false`
- <a id="s-93eb056028"></a>`required`: `["departure_id","target_identity","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e77e81c473"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cb37db66b0"></a>`format` | no | type="string"; const="stove0-departure-effect-receipt/v1"; default="stove0-departure-effect-receipt/v1" |  |
| <a id="s-d75bdeccda"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c092997b4f"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-cb6a47750c)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-departure-effect-receipt"} |  |
| <a id="s-8f4041f0c5"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-cb6a47750c"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [exact_stage](stove0-operator-contracts-departureeffectview-exact-stage.md)

## Governing policies

- <a id="pa-fc78ac7a44"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DepartureEffectView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2e0b58fb18b9edd3f4cd026397dbd9b803d4dcb2050352149a93dfb7dd2dd2d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CatalogSyncDescriptor": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "description": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionDescription"
                },
                {
                  "type": "null"
                }
              ]
            },
            "description_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "description_revision": {
              "maximum": 9007199254740991,
              "minimum": 0,
              "type": "integer"
            },
            "revision": {
              "maxLength": 19,
              "minLength": 1,
              "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
              "type": "string"
            },
            "tag_revision": {
              "maximum": 9007199254740991,
              "minimum": 1,
              "type": "integer"
            },
            "tag_set_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "description",
            "description_revision",
            "description_identity",
            "tag_revision",
            "tag_set_identity",
            "revision"
          ],
          "type": "object"
        },
        "CollectionDescription": {
          "maxLength": 32768,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 32768,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-catalog-description"
          },
          "x-unicode-normalization": "NFC"
        },
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        },
        "DepartureEffectIntent": {
          "additionalProperties": false,
          "properties": {
            "authorization_view_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "departure_cause": {
              "enum": [
                "collection_deleted",
                "visibility_lost"
              ],
              "type": "string"
            },
            "departure_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "departure_revision": {
              "maxLength": 19,
              "minLength": 1,
              "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
              "type": "string"
            },
            "format": {
              "const": "stove0-departure-effect-intent/v1",
              "default": "stove0-departure-effect-intent/v1",
              "type": "string"
            },
            "last_collection": {
              "$ref": "#/$defs/CatalogSyncDescriptor"
            },
            "policy_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "policy_revision": {
              "minimum": 1,
              "type": "integer"
            },
            "policy_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "source_identity": {
              "maxLength": 64,
              "minLength": 64,
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
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
            "policy_id",
            "policy_revision",
            "policy_sha256",
            "target_registration_id",
            "target_identity",
            "source_identity",
            "authorization_view_identity",
            "last_collection",
            "departure_cause",
            "departure_revision",
            "departure_id"
          ],
          "type": "object"
        },
        "DepartureEffectReceipt": {
          "additionalProperties": false,
          "properties": {
            "departure_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "format": {
              "const": "stove0-departure-effect-receipt/v1",
              "default": "stove0-departure-effect-receipt/v1",
              "type": "string"
            },
            "receipt_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "result": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object",
              "x-riverhog-encoded-bytes-max": 65536,
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-departure-effect-receipt"
              }
            },
            "target_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "departure_id",
            "target_identity",
            "result",
            "receipt_sha256"
          ],
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "attempt_count": {
          "minimum": 0,
          "type": "integer"
        },
        "created_at": {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        "failure": {
          "anyOf": [
            {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "intent": {
          "$ref": "#/$defs/DepartureEffectIntent"
        },
        "next_attempt_at": {
          "anyOf": [
            {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "receipt": {
          "anyOf": [
            {
              "$ref": "#/$defs/DepartureEffectReceipt"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "state": {
          "enum": [
            "pending",
            "complete"
          ],
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
        "intent",
        "state",
        "attempt_count",
        "created_at",
        "updated_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, intent: stove0_target_protocol.departure.DepartureEffectIntent, state: Literal['pending', 'complete'], receipt: stove0_target_protocol.departure.DepartureEffectReceipt | None = None, attempt_count: Annotated[int, Ge(ge=0)], next_attempt_at: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]] = None, failure: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=1000)] = None, created_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], updated_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "DepartureEffectView",
  "unit": "export"
}
```

</details>
