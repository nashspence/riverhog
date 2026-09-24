# riverhog_storage_adapter_protocol.ObjectMetadataReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectm-d3d9685026:f026627130 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-916130124e"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, content_type: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=255)] = None, stored_bytes: NonnegativeDecimal, stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observed_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement_policy: Literal['archive_default', 'immediate_default'], completed_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""`

#### Validated model schema

<a id="s-fc35800db0"></a>

- <a id="s-c5b1d384c5"></a>`type`: `"object"`
- <a id="s-f03025a7b6"></a>`additionalProperties`: `false`
- <a id="s-ee522613f9"></a>`required`: `["object_path","stored_bytes","observed_identity_assertions","verified_placement_policy","completed_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00acfbcadc"></a>`completed_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-cde06a44fe"></a>`content_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-9677d60952"></a>`entity_token` | no | anyOf=[(type="string"; maxLength=4000; minLength=1); (type="null")]; default=null |  |
| <a id="s-c3f8823a41"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-cd09a53850"></a>`observed_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |
| <a id="s-066c38dc7c"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null |  |
| <a id="s-e2d56684bf"></a>`stored_bytes` | yes | [NonnegativeDecimal](#s-9e68c8b860) |  |
| <a id="s-c7938f3880"></a>`stored_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-820715202e"></a>`verified_placement_policy` | yes | type="string"; enum=["archive_default","immediate_default"] |  |

##### Definitions

- [NonnegativeDecimal](#s-9e68c8b860)

##### <a id="s-9e68c8b860"></a>definition `NonnegativeDecimal`

- <a id="s-756b6aba21"></a>`type`: `"string"`
- <a id="s-6f988972f2"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_metadata](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-metadata.md)
- [canonical_path](riverhog-storage-adapter-protocol-objectmetadatareceipt-canonical-path.md)

## Governing policies

- <a id="pa-24799e3c3e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectMetadataReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4785d01d760d97847238bc04bd17c3e08bed4d98cb813627e00e70bbeba4e549 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "completed_at": {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
          "$ref": "#/$defs/NonnegativeDecimal"
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
        "verified_placement_policy": {
          "enum": [
            "archive_default",
            "immediate_default"
          ],
          "type": "string"
        }
      },
      "required": [
        "object_path",
        "stored_bytes",
        "observed_identity_assertions",
        "verified_placement_policy",
        "completed_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, content_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None, stored_bytes: NonnegativeDecimal, stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, observed_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement_policy: Literal['archive_default', 'immediate_default'], completed_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectMetadataReceipt",
  "unit": "export"
}
```

</details>
