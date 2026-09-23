# riverhog_protocol.CollectionUploadRegistrationConstraintsDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadregistr-54809f5fa2:a422dac2f2 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-88567d1d56"></a>`signature`: `"'(*, pack_member_bytes: Annotated[NonnegativeDecimal, Ge(ge=1)], raw_part_plaintext_bytes: Annotated[NonnegativeDecimal, Ge(ge=65536), MultipleOf(multiple_of=65536)]) -> None'"`

#### Validated model schema

<a id="s-3b16f56a5a"></a>

- <a id="s-274884b062"></a>`type`: `"object"`
- <a id="s-0987fb4cce"></a>`additionalProperties`: `false`
- <a id="s-3d6cc4c07a"></a>`required`: `["pack_member_bytes","raw_part_plaintext_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0d0508e0dd"></a>`pack_member_bytes` | yes | [NonnegativeDecimal](#s-c74b224bcb); ge=1 |  |
| <a id="s-e25641ebdd"></a>`raw_part_plaintext_bytes` | yes | [NonnegativeDecimal](#s-c74b224bcb); ge=65536; multiple_of=65536 |  |

##### Definitions

- [NonnegativeDecimal](#s-c74b224bcb)

##### <a id="s-c74b224bcb"></a>definition `NonnegativeDecimal`

- <a id="s-d0b02aa719"></a>`type`: `"string"`
- <a id="s-f450c85854"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-db5ce69555"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRegistrationConstraintsDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84decf914cfa93269e8e29f3b21dbc7e9dc47353b4e3426355b316f1baec6cbc -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "pack_member_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "raw_part_plaintext_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 65536,
          "multiple_of": 65536
        }
      },
      "required": [
        "pack_member_bytes",
        "raw_part_plaintext_bytes"
      ],
      "type": "object"
    },
    "signature": "'(*, pack_member_bytes: Annotated[NonnegativeDecimal, Ge(ge=1)], raw_part_plaintext_bytes: Annotated[NonnegativeDecimal, Ge(ge=65536), MultipleOf(multiple_of=65536)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRegistrationConstraintsDocument",
  "unit": "export"
}
```

</details>
