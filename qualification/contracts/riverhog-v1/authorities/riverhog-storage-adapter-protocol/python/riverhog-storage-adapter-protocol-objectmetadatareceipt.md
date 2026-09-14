# riverhog_storage_adapter_protocol.ObjectMetadataReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectm-d3d9685026:f026627130 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6bd7e4662"></a>
- <a id="s-4b684e98e6"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-310b6feb81"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-9e04636697"></a>`name`: `ObjectMetadataReceipt`
- <a id="s-d077994d56"></a>`unit`: `export`

### Declared structure

- <a id="s-8f29a8307d"></a>`kind`: `"class"`
- <a id="s-916130124e"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, content_type: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=255)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observed_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""`

#### Validated model schema

<a id="s-fc35800db0"></a>
- <a id="s-c5b1d384c5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00acfbcadc"></a>`completed_at` | yes | type="string"; minLength=1; maxLength=100 |  |
| <a id="s-cde06a44fe"></a>`content_type` | no | anyOf=type="string"; minLength=1; maxLength=255 \| type="null" |  |
| <a id="s-9677d60952"></a>`entity_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |
| <a id="s-c3f8823a41"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-cd09a53850"></a>`observed_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-066c38dc7c"></a>`revision` | no | anyOf=type="string"; minLength=1; maxLength=2000 \| type="null" |  |
| <a id="s-e2d56684bf"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c7938f3880"></a>`stored_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-749f31ee61"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"] |  |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_completed_at](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-completed-at.md)
- [riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_metadata](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-metadata.md)
- [riverhog_storage_adapter_protocol.ObjectMetadataReceipt.canonical_path](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-path.md)

## Governing policies

- <a id="pa-24799e3c3e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectMetadataReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17c44ec13a7ddef69505d7762941eb32a319e66fdedc9e3640e9c97ecf610afb -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "completed_at": {
          "maxLength": 100,
          "minLength": 1,
          "type": "string"
        },
        "content_type": {
          "anyOf": [
            {
              "maxLength": 255,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "entity_token": {
          "anyOf": [
            {
              "maxLength": 4000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "observed_identity_assertions": {
          "additionalProperties": {
            "type": "string"
          },
          "maxProperties": 64,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 16384,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-object-identity-assertion-envelope"
          }
        },
        "revision": {
          "anyOf": [
            {
              "maxLength": 2000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "stored_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "stored_sha256": {
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
        "verified_placement": {
          "enum": [
            "archive",
            "immediate"
          ],
          "type": "string"
        }
      },
      "required": [
        "object_path",
        "stored_bytes",
        "observed_identity_assertions",
        "verified_placement",
        "completed_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, content_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observed_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectMetadataReceipt",
  "unit": "export"
}
```
