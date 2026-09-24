# stove0_target_protocol.DepartureEffectIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectintent:0984eb4d3b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d0ca6a262"></a>
- <a id="s-2c8e87614e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-ce9bca878e"></a>`module`: `stove0_target_protocol`
- <a id="s-8e96cc4c36"></a>`name`: `DepartureEffectIntent`
- <a id="s-9dc11b4bf0"></a>`unit`: `export`

### Declared structure

- <a id="s-dabc822104"></a>`kind`: `"class"`
- <a id="s-45ceef3cd4"></a>`signature`: `"\"(*, format: Literal['stove0-departure-effect-intent/v1'] = 'stove0-departure-effect-intent/v1', policy_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_registration_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], last_collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, departure_cause: Literal['collection_deleted', 'visibility_lost'], departure_revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$', ascii_only=None)], departure_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-c61a4282b1"></a>

- <a id="s-4ff04a3177"></a>`type`: `"object"`
- <a id="s-ca129ee553"></a>`additionalProperties`: `false`
- <a id="s-41c22bc46e"></a>`required`: `["policy_id","policy_revision","policy_sha256","target_registration_id","target_identity","source_identity","authorization_view_identity","last_collection","departure_cause","departure_revision","departure_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cf24a371b3"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-204b9e6ce7"></a>`departure_cause` | yes | type="string"; enum=["collection_deleted","visibility_lost"] |  |
| <a id="s-9dd811db16"></a>`departure_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ed639b9e7c"></a>`departure_revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-238de1c915"></a>`format` | no | type="string"; const="stove0-departure-effect-intent/v1"; default="stove0-departure-effect-intent/v1" |  |
| <a id="s-7cbdcc0a45"></a>`last_collection` | yes | [CatalogSyncDescriptor](#s-221f072e3f) |  |
| <a id="s-7dec1ef195"></a>`policy_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-42b54474b6"></a>`policy_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-924584eebe"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2fe6bc3dff"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2ed1c3fa65"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5758586ba9"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1 |  |

##### Definitions

- [CatalogSyncDescriptor](#s-221f072e3f)
- [CollectionDescription](#s-7f4bde062e)
- [CollectionId](#s-874e9da459)

##### <a id="s-221f072e3f"></a>definition `CatalogSyncDescriptor`

- <a id="s-edbcaf7ed9"></a>`type`: `"object"`
- <a id="s-08cd4c0520"></a>`additionalProperties`: `false`
- <a id="s-e652b63bd6"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95ee682ca4"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8df90228a0"></a>`collection_id` | yes | [CollectionId](#s-874e9da459) |  |
| <a id="s-aceeacc46e"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f40408c5b0"></a>`description` | yes | anyOf=[([CollectionDescription](#s-7f4bde062e)); (type="null")] |  |
| <a id="s-959ab5ab26"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-31f163c387"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-a471f25549"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-41bf017b1d"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-f6c32e0f9c"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-7f4bde062e"></a>definition `CollectionDescription`

- <a id="s-996da6f66a"></a>`type`: `"string"`
- <a id="s-3e4d54a5e7"></a>`maxLength`: `32768`
- <a id="s-1ccb2e6074"></a>`minLength`: `1`
- <a id="s-abec2e0d55"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-139a8eb04e"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-bb7f35f3aa"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-874e9da459"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-34c1a79a81"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-604c839aa4"></a>2 | not=(const="0") |

## Maintained corroboration

### Related interface records

- [later_than_last_seen](stove0-target-protocol-departureeffectintent-later-than-last-seen.md)
- [exact_identity](stove0-target-protocol-departureeffectintent-exact-identity.md)
- [seal](stove0-target-protocol-departureeffectintent-seal.md)

## Governing policies

- <a id="pa-274fae06ad"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7600bb1fb787c017501cd593984092c6ff459afad3873b2d38e917035f9ef78c -->

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
        }
      },
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
    "signature": "\"(*, format: Literal['stove0-departure-effect-intent/v1'] = 'stove0-departure-effect-intent/v1', policy_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], policy_revision: Annotated[int, Ge(ge=1)], policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_registration_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], target_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], last_collection: riverhog_protocol.catalog_sync.CatalogSyncDescriptor, departure_cause: Literal['collection_deleted', 'visibility_lost'], departure_revision: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$', ascii_only=None)], departure_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "DepartureEffectIntent",
  "unit": "export"
}
```

</details>
