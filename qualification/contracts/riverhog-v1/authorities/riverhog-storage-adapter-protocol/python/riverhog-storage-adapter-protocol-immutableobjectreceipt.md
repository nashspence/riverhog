# riverhog_storage_adapter_protocol.ImmutableObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-immutab-95787d4876:19cc2b5cdd -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-c756a1ecb4"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""`

#### Validated model schema

<a id="s-1f52414858"></a>
- <a id="s-4a5c7d34d2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9a7d565fdc"></a>`completed_at` | yes | type="string"; minLength=1; maxLength=100 |  |
| <a id="s-96687cf2cf"></a>`entity_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |
| <a id="s-2a9d11f646"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-edab58366b"></a>`revision` | no | anyOf=type="string"; minLength=1; maxLength=2000 \| type="null" |  |
| <a id="s-bcb23637a5"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-a6191dde3e"></a>`stored_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1f0efb195e"></a>`verified_content_type` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-ebdea37c08"></a>`verified_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-d850b34b34"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"] |  |

## Maintained corroboration

### Related interface records

- [canonical_completed_at](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-completed-at.md)
- [canonical_path](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-path.md)
- [canonical_metadata](riverhog-storage-adapter-protocol-immutableobjectreceipt-canonical-metadata.md)

## Governing policies

- <a id="pa-8743403251"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ImmutableObjectReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff2afe2b35054334865ce03d0052d2e2f4c2bb10b466029d147ccfccf15882e3 -->

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
          "minimum": 0,
          "type": "integer"
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
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ImmutableObjectReceipt",
  "unit": "export"
}
```
