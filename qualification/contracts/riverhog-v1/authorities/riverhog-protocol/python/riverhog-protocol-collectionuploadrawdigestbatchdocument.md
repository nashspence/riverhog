# riverhog_protocol.CollectionUploadRawDigestBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawdige-f88d40f36d:78a1cd9117 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55069b7ab0"></a>
- <a id="s-a88c1544b6"></a>`distribution`: `riverhog-protocol`
- <a id="s-4ed6434195"></a>`module`: `riverhog_protocol`
- <a id="s-85798758e2"></a>`name`: `CollectionUploadRawDigestBatchDocument`
- <a id="s-d6e05578ec"></a>`unit`: `export`

### Declared structure

- <a id="s-e9c655a6f8"></a>`kind`: `"class"`
- <a id="s-598182523e"></a>`signature`: `"\"(*, path: str, first_part: Annotated[int, Strict(strict=True), Ge(ge=0)], sha256s: Annotated[list[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]], MinLen(min_length=1), MaxLen(max_length=1024)]) -> None\""`

#### Validated model schema

<a id="s-0916434e96"></a>

- <a id="s-c7c5a50ae9"></a>`type`: `"object"`
- <a id="s-2b59f0fc8d"></a>`additionalProperties`: `false`
- <a id="s-6ee88be58a"></a>`required`: `["path","first_part","sha256s"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac6b8dd81c"></a>`first_part` | yes | type="integer"; minimum=0 |  |
| <a id="s-a44cdb5394"></a>`path` | yes | type="string" |  |
| <a id="s-4ea3a086a9"></a>`sha256s` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$"); maxItems=1024; minItems=1; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"first_part","reason":"bounded-raw-digest-append"} |  |

## Maintained corroboration

### Related interface records

- [canonical_path](riverhog-protocol-collectionuploadrawdigestbatchdocument-canonical-path.md)

## Governing policies

- <a id="pa-6c7285cf7f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawDigestBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dca8ecf0b63cfd5d88fa14f4d8032d9c1f4d45c97167339d6daf7d343c974608 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "first_part": {
          "minimum": 0,
          "type": "integer"
        },
        "path": {
          "type": "string"
        },
        "sha256s": {
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "maxItems": 1024,
          "minItems": 1,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "first_part",
            "reason": "bounded-raw-digest-append"
          }
        }
      },
      "required": [
        "path",
        "first_part",
        "sha256s"
      ],
      "type": "object"
    },
    "signature": "\"(*, path: str, first_part: Annotated[int, Strict(strict=True), Ge(ge=0)], sha256s: Annotated[list[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]], MinLen(min_length=1), MaxLen(max_length=1024)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawDigestBatchDocument",
  "unit": "export"
}
```

</details>
