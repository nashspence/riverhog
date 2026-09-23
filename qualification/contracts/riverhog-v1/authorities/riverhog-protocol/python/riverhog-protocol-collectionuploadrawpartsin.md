# riverhog_protocol.CollectionUploadRawPartsIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadrawpartsin:c5e09814c6 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-4c88f3668f"></a>`signature`: `"\"(*, part_plaintext_bytes: Annotated[NonnegativeDecimal, Ge(ge=65536)], part_count: Annotated[NonnegativeDecimal, Ge(ge=1)], ordered_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-e98b5f0d86"></a>

- <a id="s-42c55b9d54"></a>`type`: `"object"`
- <a id="s-a059667b69"></a>`additionalProperties`: `false`
- <a id="s-7610d896ba"></a>`required`: `["part_plaintext_bytes","part_count","ordered_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69aaeec32c"></a>`ordered_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eef617ea2c"></a>`part_count` | yes | [NonnegativeDecimal](#s-ffc3894411); ge=1 |  |
| <a id="s-414ee80a09"></a>`part_plaintext_bytes` | yes | [NonnegativeDecimal](#s-ffc3894411); ge=65536 |  |

##### Definitions

- [NonnegativeDecimal](#s-ffc3894411)

##### <a id="s-ffc3894411"></a>definition `NonnegativeDecimal`

- <a id="s-fd51d5a93c"></a>`type`: `"string"`
- <a id="s-981fc36adb"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-85e0fb4666"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadRawPartsIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 013d00798b254d27b89d0c40264dd019657c9073f8828cb5cdbf5cbee25d81e0 -->

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
        "ordered_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "part_count": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "part_plaintext_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 65536
        }
      },
      "required": [
        "part_plaintext_bytes",
        "part_count",
        "ordered_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, part_plaintext_bytes: Annotated[NonnegativeDecimal, Ge(ge=65536)], part_count: Annotated[NonnegativeDecimal, Ge(ge=1)], ordered_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadRawPartsIn",
  "unit": "export"
}
```

</details>
