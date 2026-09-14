# riverhog_protocol.CollectionUploadRegistrationConstraintsDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadregistr-54809f5fa2:a422dac2f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-08808e03e1"></a>
- <a id="s-3fab0c3210"></a>`distribution`: `riverhog-protocol`
- <a id="s-39f68a6bca"></a>`module`: `riverhog_protocol`
- <a id="s-882861bb83"></a>`name`: `CollectionUploadRegistrationConstraintsDocument`
- <a id="s-b8df20de32"></a>`unit`: `export`

### Declared structure

- <a id="s-bb6cc5c879"></a>`kind`: `"class"`
- <a id="s-88567d1d56"></a>`signature`: `"'(*, pack_member_bytes: Annotated[int, Ge(ge=1)], raw_part_plaintext_bytes: Annotated[int, Ge(ge=65536), MultipleOf(multiple_of=65536)]) -> None'"`

#### Validated model schema

<a id="s-3b16f56a5a"></a>
- <a id="s-9c54cf2c41"></a>`title`: CollectionUploadRegistrationConstraintsDocument
- <a id="s-723e0bfce2"></a>`description`: Producer constraints issued by Riverhog for one upload session.
- <a id="s-274884b062"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0d0508e0dd"></a>`pack_member_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-e25641ebdd"></a>`raw_part_plaintext_bytes` | yes | type="integer"; minimum=65536; additional keys=`multipleOf` |  |

## Governing policies

- <a id="pa-db5ce69555"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRegistrationConstraintsDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa038536786bc8d06fc315502dddb05fcb8cd9a9b2a5379e4b33a318d8528b75 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Producer constraints issued by Riverhog for one upload session.",
      "properties": {
        "pack_member_bytes": {
          "minimum": 1,
          "title": "Pack Member Bytes",
          "type": "integer"
        },
        "raw_part_plaintext_bytes": {
          "minimum": 65536,
          "multipleOf": 65536,
          "title": "Raw Part Plaintext Bytes",
          "type": "integer"
        }
      },
      "required": [
        "pack_member_bytes",
        "raw_part_plaintext_bytes"
      ],
      "title": "CollectionUploadRegistrationConstraintsDocument",
      "type": "object"
    },
    "signature": "'(*, pack_member_bytes: Annotated[int, Ge(ge=1)], raw_part_plaintext_bytes: Annotated[int, Ge(ge=65536), MultipleOf(multiple_of=65536)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRegistrationConstraintsDocument",
  "unit": "export"
}
```
