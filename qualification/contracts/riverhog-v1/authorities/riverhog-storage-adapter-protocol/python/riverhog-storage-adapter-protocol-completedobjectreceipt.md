# riverhog_storage_adapter_protocol.CompletedObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-b3f1a3c579:cb4bdd68b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7080e1a03c"></a>
- <a id="s-bcf17ee6b6"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-50748a183c"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-b522b51ac3"></a>`name`: `CompletedObjectReceipt`
- <a id="s-d148539706"></a>`unit`: `export`

### Declared structure

- <a id="s-a7a58809dc"></a>`kind`: `"class"`
- <a id="s-75bd69e572"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: PositiveDecimal, verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""`

#### Validated model schema

<a id="s-e49af6a44f"></a>

- <a id="s-9c6edd0b86"></a>`type`: `"object"`
- <a id="s-7a13382623"></a>`additionalProperties`: `false`
- <a id="s-e3d5d357ae"></a>`required`: `["object_path","stored_bytes","verified_content_type","verified_identity_assertions","verified_placement","completed_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4cb2f85567"></a>`completed_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-ca77fdd25d"></a>`entity_token` | no | anyOf=[(type="string"; maxLength=4000; minLength=1); (type="null")]; default=null |  |
| <a id="s-3d56fe7991"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-6172d99bd4"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null |  |
| <a id="s-1e07875f37"></a>`stored_bytes` | yes | [PositiveDecimal](#s-f366680e78) |  |
| <a id="s-aa2f38771f"></a>`verified_content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-0aea214e3f"></a>`verified_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |
| <a id="s-fb208f5363"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"] |  |

##### Definitions

- [PositiveDecimal](#s-f366680e78)

##### <a id="s-f366680e78"></a>definition `PositiveDecimal`

- <a id="s-3c5f1dfc1e"></a>`type`: `"string"`
- <a id="s-3b94f17c18"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_path](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-path.md)
- [canonical_metadata](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-metadata.md)

## Governing policies

- <a id="pa-6d4b7142ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedObjectReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d4e022a2a8d85f1facde0485a0c610c767434118cf31da06cf0def8ce749baf -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "PositiveDecimal": {
          "pattern": "^[1-9][0-9]*(?![\\s\\S])",
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
          "$ref": "#/$defs/PositiveDecimal"
        },
        "verified_content_type": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "verified_identity_assertions": {
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
        "verified_content_type",
        "verified_identity_assertions",
        "verified_placement",
        "completed_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: PositiveDecimal, verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "CompletedObjectReceipt",
  "unit": "export"
}
```

</details>
