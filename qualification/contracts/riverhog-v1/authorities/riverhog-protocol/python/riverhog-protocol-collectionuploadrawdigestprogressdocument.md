# riverhog_protocol.CollectionUploadRawDigestProgressDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawdige-ed822c7f86:69ecca9f06 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e79d55b1f"></a>
- <a id="s-5d0b35929b"></a>`distribution`: `riverhog-protocol`
- <a id="s-5dede16988"></a>`module`: `riverhog_protocol`
- <a id="s-4cbbe4945c"></a>`name`: `CollectionUploadRawDigestProgressDocument`
- <a id="s-9d6a624bd9"></a>`unit`: `export`

### Declared structure

- <a id="s-4d557fcb28"></a>`kind`: `"class"`
- <a id="s-5370ee5641"></a>`signature`: `"'(*, path: str, accepted_parts: Annotated[int, Strict(strict=True), Ge(ge=0)], expected_parts: Annotated[int, Strict(strict=True), Ge(ge=1)], complete: bool) -> None'"`

#### Validated model schema

<a id="s-7b6db1bda1"></a>

- <a id="s-cd26ee3d7e"></a>`type`: `"object"`
- <a id="s-743ce9e496"></a>`additionalProperties`: `false`
- <a id="s-c87d3f05bc"></a>`required`: `["path","accepted_parts","expected_parts","complete"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e774600c7"></a>`accepted_parts` | yes | type="integer"; minimum=0 |  |
| <a id="s-ca9a90315b"></a>`complete` | yes | type="boolean" |  |
| <a id="s-97f2b7dccc"></a>`expected_parts` | yes | type="integer"; minimum=1 |  |
| <a id="s-462cb70bff"></a>`path` | yes | type="string" |  |

## Maintained corroboration

### Related interface records

- [canonical_path](riverhog-protocol-collectionuploadrawdigestprogressdocument-canonical-path.md)
- [validate_completion](riverhog-protocol-collectionuploadrawdigestprogressdocument-validate-completion.md)

## Governing policies

- <a id="pa-fb918a1fae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawDigestProgressDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26018243cefc6529cf62fb094dda274575ee193bf0e27dab6f5b442eebaca3a4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "accepted_parts": {
          "minimum": 0,
          "type": "integer"
        },
        "complete": {
          "type": "boolean"
        },
        "expected_parts": {
          "minimum": 1,
          "type": "integer"
        },
        "path": {
          "type": "string"
        }
      },
      "required": [
        "path",
        "accepted_parts",
        "expected_parts",
        "complete"
      ],
      "type": "object"
    },
    "signature": "'(*, path: str, accepted_parts: Annotated[int, Strict(strict=True), Ge(ge=0)], expected_parts: Annotated[int, Strict(strict=True), Ge(ge=1)], complete: bool) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawDigestProgressDocument",
  "unit": "export"
}
```

</details>
