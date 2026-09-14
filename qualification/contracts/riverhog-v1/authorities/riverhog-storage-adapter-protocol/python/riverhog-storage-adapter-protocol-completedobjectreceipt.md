# riverhog_storage_adapter_protocol.CompletedObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-b3f1a3c579:cb4bdd68b2 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-75bd69e572"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=1)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""`

#### Validated model schema

<a id="s-e49af6a44f"></a>
- <a id="s-9c6edd0b86"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4cb2f85567"></a>`completed_at` | yes | type="string"; minLength=1; maxLength=100 |  |
| <a id="s-ca77fdd25d"></a>`entity_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |
| <a id="s-3d56fe7991"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-6172d99bd4"></a>`revision` | no | anyOf=type="string"; minLength=1; maxLength=2000 \| type="null" |  |
| <a id="s-1e07875f37"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-aa2f38771f"></a>`verified_content_type` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-0aea214e3f"></a>`verified_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-fb208f5363"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"] |  |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_completed_at](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-completed-at.md)
- [riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_path](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-path.md)
- [riverhog_storage_adapter_protocol.CompletedObjectReceipt.canonical_metadata](riverhog-storage-adapter-protocol-completedobjectreceipt-canonical-metadata.md)

## Governing policies

- <a id="pa-6d4b7142ab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedObjectReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e48f516de132f1c8e43c82f1fbd0e60ca3d64124f5887da07c692890a5db8fba -->

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
          "minimum": 1,
          "type": "integer"
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
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], revision: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=2000)] = None, entity_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, stored_bytes: Annotated[int, Ge(ge=1)], verified_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], verified_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], verified_placement: Literal['archive', 'immediate'], completed_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "CompletedObjectReceipt",
  "unit": "export"
}
```
