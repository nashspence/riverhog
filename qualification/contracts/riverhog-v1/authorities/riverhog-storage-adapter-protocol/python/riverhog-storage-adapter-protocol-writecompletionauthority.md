# riverhog_storage_adapter_protocol.WriteCompletionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-220f6362b5:f90fbdd150 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-05ee202efc"></a>
- <a id="s-6601d0b76d"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-9f45d619fc"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-55ab86e4a3"></a>`name`: `WriteCompletionAuthority`
- <a id="s-7c78dfeffd"></a>`unit`: `export`

### Declared structure

- <a id="s-91bb80734b"></a>`kind`: `"class"`
- <a id="s-d1fd26e4cc"></a>`signature`: `"'(*, segment_count: NonnegativeDecimal, stored_bytes: NonnegativeDecimal, authority_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"`

#### Validated model schema

<a id="s-5affb1de34"></a>

- <a id="s-e2273c32f4"></a>`type`: `"object"`
- <a id="s-978f8ffa02"></a>`additionalProperties`: `false`
- <a id="s-8238f02ee7"></a>`required`: `["segment_count","stored_bytes","authority_token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b94bef0b17"></a>`authority_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-1eb93a177f"></a>`segment_count` | yes | [NonnegativeDecimal](#s-6f20695bd7) |  |
| <a id="s-45cd639d4b"></a>`stored_bytes` | yes | [NonnegativeDecimal](#s-6f20695bd7) |  |

##### Definitions

- [NonnegativeDecimal](#s-6f20695bd7)

##### <a id="s-6f20695bd7"></a>definition `NonnegativeDecimal`

- <a id="s-944dbf2080"></a>`type`: `"string"`
- <a id="s-7a62c5822c"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-38dff110ff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompletionAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb39b9a1712a1982840648a6a0dd1c47f6a79bfce3f3d5b3d206fef04740ee1f -->

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
        "authority_token": {
          "maxLength": 4000,
          "minLength": 1,
          "type": "string"
        },
        "segment_count": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "stored_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
        }
      },
      "required": [
        "segment_count",
        "stored_bytes",
        "authority_token"
      ],
      "type": "object"
    },
    "signature": "'(*, segment_count: NonnegativeDecimal, stored_bytes: NonnegativeDecimal, authority_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteCompletionAuthority",
  "unit": "export"
}
```

</details>
