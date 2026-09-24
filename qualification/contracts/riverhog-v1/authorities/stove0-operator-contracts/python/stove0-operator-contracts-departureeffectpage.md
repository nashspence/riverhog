# stove0_operator_contracts.DepartureEffectPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departureeffectpage:477e01e036 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-032a1cfd22"></a>
- <a id="s-fd7ac046e8"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-729806919e"></a>`module`: `stove0_operator_contracts`
- <a id="s-142b1a4063"></a>`name`: `DepartureEffectPage`
- <a id="s-e6f6efe75f"></a>`unit`: `export`

### Declared structure

- <a id="s-afea666971"></a>`kind`: `"class"`
- <a id="s-86058cc89d"></a>`signature`: `"'(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken \| None, effects: Annotated[tuple[stove0_operator_contracts.DepartureEffectView, ...], MaxLen(max_length=100)]) -> None'"`

#### Validated model schema

<a id="s-0d01a865d3"></a>

- <a id="s-603b916729"></a>`type`: `"object"`
- <a id="s-f0f6f7d45b"></a>`additionalProperties`: `false`
- <a id="s-e9ea281502"></a>`required`: `["page_size","next_page_token","effects"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1dd1d3adb"></a>`effects` | yes | type="array"; items=([DepartureEffectView](#s-a7584621e0)); maxItems=100; x-riverhog-extent={"policy":"contract_max","reason":"bounded-departure-effect-browse-page"} |  |
| <a id="s-b063999ad4"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](#s-288a69307b)); (type="null")] |  |
| <a id="s-6b12829c04"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |

##### Definitions

- [BrowsePageToken](#s-288a69307b)
- [CatalogSyncDescriptor](#s-7311eb077b)
- [CollectionDescription](#s-daeae1a1d9)
- [CollectionId](#s-748fe297ee)
- [DepartureEffectIntent](#s-d90be18a22)
- [DepartureEffectReceipt](#s-b2e6e16e0d)
- [DepartureEffectView](#s-a7584621e0)
- [JsonValue](#s-7df2e27e94)

##### <a id="s-288a69307b"></a>definition `BrowsePageToken`

- <a id="s-95f384fbee"></a>`type`: `"string"`
- <a id="s-090cd57238"></a>`maxLength`: `8192`
- <a id="s-d8386815c2"></a>`minLength`: `1`

##### <a id="s-7311eb077b"></a>definition `CatalogSyncDescriptor`

- <a id="s-a13dad03cc"></a>`type`: `"object"`
- <a id="s-5fe3608314"></a>`additionalProperties`: `false`
- <a id="s-3deb2d0f4c"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e0f6e8f525"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1578ee0bb8"></a>`collection_id` | yes | [CollectionId](#s-748fe297ee) |  |
| <a id="s-f8d7cfe0e2"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8d63fdf84a"></a>`description` | yes | anyOf=[([CollectionDescription](#s-daeae1a1d9)); (type="null")] |  |
| <a id="s-5f4909c1f0"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-84e85131ec"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-9ba57bac99"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-a90438a419"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-df093d9e54"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-daeae1a1d9"></a>definition `CollectionDescription`

- <a id="s-80c1b4ee7e"></a>`type`: `"string"`
- <a id="s-6c396882c2"></a>`maxLength`: `32768`
- <a id="s-bdcca6cefc"></a>`minLength`: `1`
- <a id="s-9b79fda33f"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-f8a8bec653"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-9ab73854b6"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-748fe297ee"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-c158eb1b1f"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-6e99a49a4b"></a>2 | not=(const="0") |

##### <a id="s-d90be18a22"></a>definition `DepartureEffectIntent`

- <a id="s-fea0e5dace"></a>`type`: `"object"`
- <a id="s-0cbfc9dda8"></a>`additionalProperties`: `false`
- <a id="s-387b780733"></a>`required`: `["policy_id","policy_revision","policy_sha256","target_registration_id","target_identity","source_identity","authorization_view_identity","last_collection","departure_cause","departure_revision","departure_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69ab995a9c"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-74cb1e1f5f"></a>`departure_cause` | yes | type="string"; enum=["collection_deleted","visibility_lost"] |  |
| <a id="s-8f44463a1e"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-30c8c37dfb"></a>`departure_revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-cc0c93b5c0"></a>`format` | no | type="string"; const="stove0-departure-effect-intent/v1"; default="stove0-departure-effect-intent/v1" |  |
| <a id="s-bd57a03892"></a>`last_collection` | yes | [CatalogSyncDescriptor](#s-7311eb077b) |  |
| <a id="s-8ef19272c3"></a>`policy_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d229434083"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-94af19dc08"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-83f74c0d2c"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-65b2b40c7a"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-327e7cea0f"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### <a id="s-b2e6e16e0d"></a>definition `DepartureEffectReceipt`

- <a id="s-f87095d5b7"></a>`type`: `"object"`
- <a id="s-ba781563cd"></a>`additionalProperties`: `false`
- <a id="s-58cf579555"></a>`required`: `["departure_id","target_identity","result","receipt_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1e135e6d7"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-13304d0329"></a>`format` | no | type="string"; const="stove0-departure-effect-receipt/v1"; default="stove0-departure-effect-receipt/v1" |  |
| <a id="s-381757c771"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5b4056a62a"></a>`result` | yes | type="object"; additionalProperties=([JsonValue](#s-7df2e27e94)); x-riverhog-encoded-bytes-max=65536; x-riverhog-extent={"policy":"contract_max","reason":"bounded-departure-effect-receipt"} |  |
| <a id="s-d44d95793d"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-a7584621e0"></a>definition `DepartureEffectView`

- <a id="s-834d0368cd"></a>`type`: `"object"`
- <a id="s-505a58a6a3"></a>`additionalProperties`: `false`
- <a id="s-efd4570599"></a>`required`: `["intent","state","attempt_count","created_at","updated_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b3ad577b49"></a>`attempt_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-38978acd05"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-e70d6f5648"></a>`failure` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-a607fc9c51"></a>`intent` | yes | [DepartureEffectIntent](#s-d90be18a22) |  |
| <a id="s-485fb84bb1"></a>`next_attempt_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-0896648e65"></a>`receipt` | no | anyOf=[([DepartureEffectReceipt](#s-b2e6e16e0d)); (type="null")]; default=null |  |
| <a id="s-c8ef54d204"></a>`state` | yes | type="string"; enum=["pending","complete"] |  |
| <a id="s-4022e266e0"></a>`updated_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |

##### <a id="s-7df2e27e94"></a>definition `JsonValue`

- Accepts: any JSON value.

## Governing policies

- <a id="pa-c78e521c54"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DepartureEffectPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a6cbad03fd00a48f7fa678761522f0344a1e42c01a214bdf1c376d1cfb518b3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "BrowsePageToken": {
          "maxLength": 8192,
          "minLength": 1,
          "type": "string"
        },
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
        "DepartureEffectView": {
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
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "effects": {
          "items": {
            "$ref": "#/$defs/DepartureEffectView"
          },
          "maxItems": 100,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-departure-effect-browse-page"
          }
        },
        "next_page_token": {
          "anyOf": [
            {
              "$ref": "#/$defs/BrowsePageToken"
            },
            {
              "type": "null"
            }
          ]
        },
        "page_size": {
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "page_size",
        "next_page_token",
        "effects"
      ],
      "type": "object"
    },
    "signature": "'(*, page_size: Annotated[int, Ge(ge=1), Le(le=100)], next_page_token: BrowsePageToken | None, effects: Annotated[tuple[stove0_operator_contracts.DepartureEffectView, ...], MaxLen(max_length=100)]) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "DepartureEffectPage",
  "unit": "export"
}
```

</details>
