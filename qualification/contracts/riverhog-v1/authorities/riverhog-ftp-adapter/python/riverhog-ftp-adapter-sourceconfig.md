# riverhog_ftp_adapter.SourceConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-sourceconfig:c1aefaa4e9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99d467f9fb"></a>
- <a id="s-d64b48fa7d"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-617284a63c"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-55281d26bd"></a>`name`: `SourceConfig`
- <a id="s-7cd33da652"></a>`unit`: `export`

### Declared structure

- <a id="s-3df8b27b82"></a>`kind`: `"class"`
- <a id="s-4d6f2904a1"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$')], root: pathlib.Path, ingest_source: Annotated[str, MinLen(min_length=1), MaxLen(max_length=512)], archive_store: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=160)] = None, description: CollectionDescription \| None = None, tags: tuple[CollectionTag, ...] = (), close_mode: Literal['stable', 'explicit-flush'] = 'stable', max_files: Annotated[int, Ge(ge=1)] = 1000, max_bytes: Annotated[int, Ge(ge=1)] = 107374182400, provenance: Literal['capture', 'omit'] = 'capture', provenance_omission_reason: Annotated[str \| None, MaxLen(max_length=1000)] = None) -> None\""`

#### Validated model schema

<a id="s-3abecbca66"></a>
- <a id="s-e0685f4790"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2f6e360bd"></a>`archive_store` | no | anyOf=type="string"; minLength=1; maxLength=160 \| type="null" |  |
| <a id="s-61be86a337"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"] |  |
| <a id="s-aa5b361399"></a>`description` | no | anyOf=#/$defs/CollectionDescription \| type="null" |  |
| <a id="s-91145a9ec7"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-4eac7856d0"></a>`ingest_source` | yes | type="string"; minLength=1; maxLength=512 |  |
| <a id="s-20a31ed4e5"></a>`max_bytes` | no | type="integer"; minimum=1 |  |
| <a id="s-d1f237a26c"></a>`max_files` | no | type="integer"; minimum=1 |  |
| <a id="s-45fe0b309b"></a>`provenance` | no | type="string"; enum=["capture","omit"] |  |
| <a id="s-61c3fd440f"></a>`provenance_omission_reason` | no | anyOf=type="string"; maxLength=1000 \| type="null" |  |
| <a id="s-23579dbc50"></a>`root` | yes | type="string"; format="path" |  |
| <a id="s-75b01acf3e"></a>`tags` | no | type="array"; items=(#/$defs/CollectionTag) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-f5f291f15b"></a>`CollectionDescription` | type="string"; minLength=1; maxLength=32768; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-7aafaec44c"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter.SourceConfig.absolute_root](riverhog-ftp-adapter-sourceconfig-absolute-root.md)
- [riverhog_ftp_adapter.SourceConfig.complete_policy](riverhog-ftp-adapter-sourceconfig-complete-policy.md)

## Governing policies

- <a id="pa-ab51d7f58a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.SourceConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e720c3915b712da371cd9ea3372fd2a3b83010ad8ff082944a2a570bfa38238 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "archive_store": {
          "anyOf": [
            {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "close_mode": {
          "default": "stable",
          "enum": [
            "stable",
            "explicit-flush"
          ],
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
          ],
          "default": null
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
          "type": "string"
        },
        "ingest_source": {
          "maxLength": 512,
          "minLength": 1,
          "type": "string"
        },
        "max_bytes": {
          "default": 107374182400,
          "minimum": 1,
          "type": "integer"
        },
        "max_files": {
          "default": 1000,
          "minimum": 1,
          "type": "integer"
        },
        "provenance": {
          "default": "capture",
          "enum": [
            "capture",
            "omit"
          ],
          "type": "string"
        },
        "provenance_omission_reason": {
          "anyOf": [
            {
              "maxLength": 1000,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "root": {
          "format": "path",
          "type": "string"
        },
        "tags": {
          "default": [],
          "items": {
            "$ref": "#/$defs/CollectionTag"
          },
          "type": "array"
        }
      },
      "required": [
        "id",
        "root",
        "ingest_source"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$')], root: pathlib.Path, ingest_source: Annotated[str, MinLen(min_length=1), MaxLen(max_length=512)], archive_store: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=160)] = None, description: CollectionDescription | None = None, tags: tuple[CollectionTag, ...] = (), close_mode: Literal['stable', 'explicit-flush'] = 'stable', max_files: Annotated[int, Ge(ge=1)] = 1000, max_bytes: Annotated[int, Ge(ge=1)] = 107374182400, provenance: Literal['capture', 'omit'] = 'capture', provenance_omission_reason: Annotated[str | None, MaxLen(max_length=1000)] = None) -> None\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "SourceConfig",
  "unit": "export"
}
```
