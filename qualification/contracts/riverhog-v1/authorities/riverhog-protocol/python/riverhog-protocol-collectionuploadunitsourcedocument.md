# riverhog_protocol.CollectionUploadUnitSourceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitsourcedocument:ec920a5b74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5a17be18e2"></a>
- <a id="s-4be13710e8"></a>`distribution`: `riverhog-protocol`
- <a id="s-760abd5d8b"></a>`module`: `riverhog_protocol`
- <a id="s-86bb11ff9f"></a>`name`: `CollectionUploadUnitSourceDocument`
- <a id="s-32196e6fd1"></a>`unit`: `export`

### Declared structure

- <a id="s-106002169d"></a>`kind`: `"class"`
- <a id="s-b092a380f0"></a>`signature`: `"\"(*, path: str, offset: Annotated[int, Strict(strict=True), Ge(ge=0)], bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], artifact_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-9fe1c6a09c"></a>
- <a id="s-8c1af34a92"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4d7d590e2b"></a>`artifact_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c10dc15699"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-390f55d49f"></a>`offset` | yes | type="integer"; minimum=0 |  |
| <a id="s-86048297b7"></a>`path` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionUploadUnitSourceDocument.canonical_path](riverhog-protocol-collectionuploadunitsourcedocument-canonical-path.md)

## Governing policies

- <a id="pa-5f6e95d62c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitSourceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c3b23f5d656a74142da55815050b1f3429869705e501fe8863f71b910f80adf -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "artifact_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "offset": {
          "minimum": 0,
          "type": "integer"
        },
        "path": {
          "type": "string"
        }
      },
      "required": [
        "path",
        "offset",
        "bytes",
        "artifact_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, path: str, offset: Annotated[int, Strict(strict=True), Ge(ge=0)], bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], artifact_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitSourceDocument",
  "unit": "export"
}
```
