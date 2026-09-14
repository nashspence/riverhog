# riverhog_protocol.CollectionUploadRawPartsIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawpartsin:c5e09814c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be78184727"></a>
- <a id="s-0d43b035c2"></a>`distribution`: `riverhog-protocol`
- <a id="s-61b8867c02"></a>`module`: `riverhog_protocol`
- <a id="s-b3a53c2e4e"></a>`name`: `CollectionUploadRawPartsIn`
- <a id="s-5f32ab1388"></a>`unit`: `export`

### Declared structure

- <a id="s-20939c8aa6"></a>`kind`: `"class"`
- <a id="s-4c88f3668f"></a>`signature`: `"\"(*, part_plaintext_bytes: Annotated[int, Ge(ge=65536)], part_count: Annotated[int, Strict(strict=True), Ge(ge=1)], ordered_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-e98b5f0d86"></a>
- <a id="s-26ed804d1c"></a>`title`: CollectionUploadRawPartsIn
- <a id="s-42c55b9d54"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69aaeec32c"></a>`ordered_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eef617ea2c"></a>`part_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-414ee80a09"></a>`part_plaintext_bytes` | yes | type="integer"; minimum=65536 |  |

## Governing policies

- <a id="pa-85e0fb4666"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawPartsIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52591dbea8ff567066fe22efbf56ae7ae662de131bcd7ad652c33890a60072d9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "ordered_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Ordered Sha256",
          "type": "string"
        },
        "part_count": {
          "minimum": 1,
          "title": "Part Count",
          "type": "integer"
        },
        "part_plaintext_bytes": {
          "minimum": 65536,
          "title": "Part Plaintext Bytes",
          "type": "integer"
        }
      },
      "required": [
        "part_plaintext_bytes",
        "part_count",
        "ordered_sha256"
      ],
      "title": "CollectionUploadRawPartsIn",
      "type": "object"
    },
    "signature": "\"(*, part_plaintext_bytes: Annotated[int, Ge(ge=65536)], part_count: Annotated[int, Strict(strict=True), Ge(ge=1)], ordered_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawPartsIn",
  "unit": "export"
}
```
