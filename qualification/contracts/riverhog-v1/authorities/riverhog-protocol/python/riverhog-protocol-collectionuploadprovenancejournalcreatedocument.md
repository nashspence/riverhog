# riverhog_protocol.CollectionUploadProvenanceJournalCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadprovena-7519864da6:5de634bc4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7b1943ecd"></a>
- <a id="s-62e4cb2e55"></a>`distribution`: `riverhog-protocol`
- <a id="s-eeef49e8cb"></a>`module`: `riverhog_protocol`
- <a id="s-b857ba0be3"></a>`name`: `CollectionUploadProvenanceJournalCreateDocument`
- <a id="s-4627a295f9"></a>`unit`: `export`

### Declared structure

- <a id="s-a9f5970ee2"></a>`kind`: `"class"`
- <a id="s-90b646475e"></a>`signature`: `"\"(*, bytes: Annotated[int, Strict(strict=True), Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-c51483f666"></a>

- <a id="s-c4f786fc96"></a>`type`: `"object"`
- <a id="s-a8dde2351a"></a>`additionalProperties`: `false`
- <a id="s-d6932e6849"></a>`required`: `["bytes","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df109aae27"></a>`bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-dfdbc4641e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-357a4da043"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadProvenanceJournalCreateDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b78e0310b90834fca31c7f3ab90d5d9c8180aa2f5d904266eed9f06c20faa381 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "bytes",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, bytes: Annotated[int, Strict(strict=True), Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadProvenanceJournalCreateDocument",
  "unit": "export"
}
```

</details>
