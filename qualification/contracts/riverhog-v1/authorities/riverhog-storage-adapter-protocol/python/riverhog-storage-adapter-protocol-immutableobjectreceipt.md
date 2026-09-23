# riverhog_storage_adapter_protocol.ImmutableObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-immutab-95787d4876:19cc2b5cdd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0cf57aa068"></a>
- <a id="s-3d3bf3a29e"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-f84e9690fc"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d6a3334737"></a>`name`: `ImmutableObjectReceipt`
- <a id="s-bb7a0b9c3d"></a>`unit`: `export`

### Declared structure

- <a id="s-9b111302ab"></a>`kind`: `"class"`
- <a id="s-c756a1ecb4"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: NonnegativeDecimal, stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""`

#### Validated model schema

<a id="s-1f52414858"></a>

- <a id="s-4a5c7d34d2"></a>`type`: `"object"`
- <a id="s-f2b8dffc7d"></a>`additionalProperties`: `false`
- <a id="s-a828f3ff72"></a>`required`: `["object_path","stored_bytes","stored_sha256","verified_content_type","verified_identity_assertions","verified_placement","completed_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9a7d565fdc"></a>`completed_at` | yes | type="string"; maxLength=100; minLength=1 |  |
| <a id="s-96687cf2cf"></a>`entity_token` | no | anyOf=[(type="string"; maxLength=4000; minLength=1); (type="null")]; default=null |  |
| <a id="s-2a9d11f646"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-edab58366b"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null |  |
| <a id="s-bcb23637a5"></a>`stored_bytes` | yes | [NonnegativeDecimal](#s-cb9619044d) |  |
| <a id="s-a6191dde3e"></a>`stored_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1f0efb195e"></a>`verified_content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-ebdea37c08"></a>`verified_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |
| <a id="s-d850b34b34"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"] |  |

##### Definitions

- [NonnegativeDecimal](#s-cb9619044d)

##### <a id="s-cb9619044d"></a>definition `NonnegativeDecimal`

- <a id="s-d275f6874a"></a>`type`: `"string"`
- <a id="s-565ff65ac3"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_completed_at](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-completed-at.md)
- [canonical_path](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-path.md)
- [canonical_metadata](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-metadata.md)

## Governing policies

- <a id="pa-8743403251"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ImmutableObjectReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 955cf57ab60b626dca5d029ce5012ef3e1bd9cda75760a1a7124f1917f62a7ce -->

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
          "maxLength": 100,
          "minLength": 1,
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
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "stored_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
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
        "stored_sha256",
        "verified_content_type",
        "verified_identity_assertions",
        "verified_placement",
        "completed_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: NonnegativeDecimal, stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ImmutableObjectReceipt",
  "unit": "export"
}
```

</details>
