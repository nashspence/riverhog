# a_riverhog_ftp_spool.SourceConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-sourceconfig:0577887a2a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99611d6eca"></a>
- <a id="s-1e93f7a6a6"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-f20a9a65b7"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-4cbc83f668"></a>`name`: `SourceConfig`
- <a id="s-ac83830e32"></a>`unit`: `export`

### Declared structure

- <a id="s-3c698dc6f0"></a>`kind`: `"class"`
- <a id="s-6dd6702acf"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$')], root: pathlib.Path, ingest_source: Annotated[str, MinLen(min_length=1), MaxLen(max_length=512)], archive_store: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=160)] = None, description: CollectionDescription \| None = None, tags: tuple[CollectionTag, ...] = (), close_mode: Literal['stable', 'explicit-flush'] = 'stable', max_files: Annotated[int, Ge(ge=1)] = 1000, max_bytes: Annotated[int, Ge(ge=1)] = 107374182400, provenance: Literal['capture', 'omit'] = 'capture', provenance_omission_reason: Annotated[str \| None, MaxLen(max_length=1000)] = None) -> None\""`

#### Validated model schema

<a id="s-7bcabac245"></a>

- <a id="s-86ab31bbde"></a>`type`: `"object"`
- <a id="s-c01542f2e6"></a>`additionalProperties`: `false`
- <a id="s-011901a102"></a>`required`: `["id","root","ingest_source"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7bd59532c6"></a>`archive_store` | no | anyOf=[(type="string"; maxLength=160; minLength=1); (type="null")]; default=null |  |
| <a id="s-b580594a1a"></a>`close_mode` | no | type="string"; enum=["stable","explicit-flush"]; default="stable" |  |
| <a id="s-0d01d67e9e"></a>`description` | no | anyOf=[([CollectionDescription](#s-b17c485ecf)); (type="null")]; default=null |  |
| <a id="s-88047cdfa5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-550c1b86ee"></a>`ingest_source` | yes | type="string"; maxLength=512; minLength=1 |  |
| <a id="s-47b6a0e1ea"></a>`max_bytes` | no | type="integer"; minimum=1; default=107374182400 |  |
| <a id="s-ac5afdec25"></a>`max_files` | no | type="integer"; minimum=1; default=1000 |  |
| <a id="s-b97472da38"></a>`provenance` | no | type="string"; enum=["capture","omit"]; default="capture" |  |
| <a id="s-1e6f75f639"></a>`provenance_omission_reason` | no | anyOf=[(type="string"; maxLength=1000); (type="null")]; default=null |  |
| <a id="s-fc41b49258"></a>`root` | yes | type="string"; format="path" |  |
| <a id="s-df5687f948"></a>`tags` | no | type="array"; default=[]; items=([CollectionTag](#s-2693ce376c)) |  |

##### Definitions

- [CollectionDescription](#s-b17c485ecf)
- [CollectionTag](#s-2693ce376c)

##### <a id="s-b17c485ecf"></a>definition `CollectionDescription`

- <a id="s-1bf7c15b1a"></a>`type`: `"string"`
- <a id="s-1a3bcded89"></a>`maxLength`: `32768`
- <a id="s-d58c48e8c2"></a>`minLength`: `1`
- <a id="s-cd935de8bb"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-e846fad751"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-fd064d261e"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-2693ce376c"></a>definition `CollectionTag`

- <a id="s-51d9b246a1"></a>`type`: `"string"`
- <a id="s-72370cfcd1"></a>`maxLength`: `65536`
- <a id="s-10998403ca"></a>`minLength`: `1`
- <a id="s-3a80543ad0"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-28fe27d08b"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-c42e9ddca5"></a>`x-unicode-normalization`: `"NFC"`

## Maintained corroboration

### Related interface records

- [absolute_root](a-riverhog-ftp-spool-sourceconfig-absolute-root.md)
- [complete_policy](a-riverhog-ftp-spool-sourceconfig-complete-policy.md)

## Governing policies

- <a id="pa-b417c207f8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.SourceConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cab3d59951fb032625a6602199e0fd3e9dd0a7bf752defea0efdc35856dbe38a -->

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
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "SourceConfig",
  "unit": "export"
}
```

</details>
