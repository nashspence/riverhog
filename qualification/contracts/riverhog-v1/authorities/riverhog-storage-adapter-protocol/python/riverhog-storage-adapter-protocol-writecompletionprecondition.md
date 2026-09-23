# riverhog_storage_adapter_protocol.WriteCompletionPrecondition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-73d6e6e85f:9497ec5e49 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b909d2f789"></a>
- <a id="s-4f0ea5739a"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-031515d09e"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d1ced17a6e"></a>`name`: `WriteCompletionPrecondition`
- <a id="s-6468c47b61"></a>`unit`: `export`

### Declared structure

- <a id="s-9d9d97a3a0"></a>`kind`: `"class"`
- <a id="s-c5fabc0da3"></a>`signature`: `"'(*, segment_count: NonnegativeDecimal, stored_bytes: NonnegativeDecimal, state_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"`

#### Validated model schema

<a id="s-2367caf51c"></a>

- <a id="s-c4bc0291ff"></a>`type`: `"object"`
- <a id="s-ffeb96633d"></a>`additionalProperties`: `false`
- <a id="s-e7ea3ddd07"></a>`required`: `["segment_count","stored_bytes","state_token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cdb304084"></a>`segment_count` | yes | [NonnegativeDecimal](#s-d162e17eb2) |  |
| <a id="s-b4db697aa3"></a>`state_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-1f15e650bd"></a>`stored_bytes` | yes | [NonnegativeDecimal](#s-d162e17eb2) |  |

##### Definitions

- [NonnegativeDecimal](#s-d162e17eb2)

##### <a id="s-d162e17eb2"></a>definition `NonnegativeDecimal`

- <a id="s-11191a85b1"></a>`type`: `"string"`
- <a id="s-147ddfe05a"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-e4e52bcd2c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompletionPrecondition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aad5657b07b4e5c6eb8577b6c1a9fc4baa636008bdd8c3d2c875815ae8b1dd68 -->

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
        "segment_count": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "state_token": {
          "maxLength": 4000,
          "minLength": 1,
          "type": "string"
        },
        "stored_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
        }
      },
      "required": [
        "segment_count",
        "stored_bytes",
        "state_token"
      ],
      "type": "object"
    },
    "signature": "'(*, segment_count: NonnegativeDecimal, stored_bytes: NonnegativeDecimal, state_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteCompletionPrecondition",
  "unit": "export"
}
```

</details>
