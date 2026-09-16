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

- <a id="s-274884b062"></a>`type`: `"object"`
- <a id="s-0987fb4cce"></a>`additionalProperties`: `false`
- <a id="s-3d6cc4c07a"></a>`required`: `["pack_member_bytes","raw_part_plaintext_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0d0508e0dd"></a>`pack_member_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-e25641ebdd"></a>`raw_part_plaintext_bytes` | yes | type="integer"; minimum=65536; multipleOf=65536 |  |

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 339097ad6f51bd455551fe62b08149583fe5d65612a9d1f2a5d013009bb5854b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "pack_member_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "raw_part_plaintext_bytes": {
          "minimum": 65536,
          "multipleOf": 65536,
          "type": "integer"
        }
      },
      "required": [
        "pack_member_bytes",
        "raw_part_plaintext_bytes"
      ],
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

</details>
