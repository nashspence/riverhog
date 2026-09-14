# riverhog_storage_adapter_protocol.WriteSegmentRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-46ea2c26f4:73e4c923d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9919f8e86c"></a>
- <a id="s-07eef77820"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-685571e12f"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d15cd24c47"></a>`name`: `WriteSegmentRequest`
- <a id="s-b7039396c9"></a>`unit`: `export`

### Declared structure

- <a id="s-ec5585b1f5"></a>`kind`: `"class"`
- <a id="s-f0c1b873f3"></a>`signature`: `"'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, number: Annotated[int, Ge(ge=1)], stored_bytes: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-af6898c92c"></a>
- <a id="s-c1cc93a30b"></a>`title`: WriteSegmentRequest
- <a id="s-8b119c136e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-29ec31d434"></a>`number` | yes | type="integer"; minimum=1 |  |
| <a id="s-a92f6bcec1"></a>`session` | yes | #/$defs/WriteSession |  |
| <a id="s-bdfbfb086e"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-3bcf6f4274"></a>`WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-eecdcac42d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c95b8415a4b28e2e19e5657774b951a6bbaf863b44b64e44cb451d57c6c0189 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "WriteSession": {
          "additionalProperties": false,
          "properties": {
            "expected_bytes": {
              "description": "Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal.",
              "minimum": 1,
              "title": "Expected Bytes",
              "type": "integer"
            },
            "object_path": {
              "maxLength": 4096,
              "minLength": 1,
              "title": "Object Path",
              "type": "string"
            },
            "write_token": {
              "description": "Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal.",
              "maxLength": 4000,
              "minLength": 1,
              "title": "Write Token",
              "type": "string"
            }
          },
          "required": [
            "object_path",
            "expected_bytes",
            "write_token"
          ],
          "title": "WriteSession",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "number": {
          "minimum": 1,
          "title": "Number",
          "type": "integer"
        },
        "session": {
          "$ref": "#/$defs/WriteSession"
        },
        "stored_bytes": {
          "minimum": 1,
          "title": "Stored Bytes",
          "type": "integer"
        }
      },
      "required": [
        "session",
        "number",
        "stored_bytes"
      ],
      "title": "WriteSegmentRequest",
      "type": "object"
    },
    "signature": "'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, number: Annotated[int, Ge(ge=1)], stored_bytes: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentRequest",
  "unit": "export"
}
```
