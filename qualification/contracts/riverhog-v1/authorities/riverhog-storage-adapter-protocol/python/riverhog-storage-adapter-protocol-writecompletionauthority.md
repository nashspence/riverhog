# riverhog_storage_adapter_protocol.WriteCompletionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-220f6362b5:f90fbdd150 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-d1fd26e4cc"></a>`signature`: `"'(*, segment_count: Annotated[int, Ge(ge=0)], stored_bytes: Annotated[int, Ge(ge=0)], authority_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"`

#### Validated model schema

<a id="s-5affb1de34"></a>

- <a id="s-e2273c32f4"></a>`type`: `"object"`
- <a id="s-978f8ffa02"></a>`additionalProperties`: `false`
- <a id="s-8238f02ee7"></a>`required`: `["segment_count","stored_bytes","authority_token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b94bef0b17"></a>`authority_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-1eb93a177f"></a>`segment_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-45cd639d4b"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-38dff110ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompletionAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21e28c7b2a0b960212fd769727eb7ab3d78213606103b2a8f3d9c32a74e2023b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "authority_token": {
          "maxLength": 4000,
          "minLength": 1,
          "type": "string"
        },
        "segment_count": {
          "minimum": 0,
          "type": "integer"
        },
        "stored_bytes": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "segment_count",
        "stored_bytes",
        "authority_token"
      ],
      "type": "object"
    },
    "signature": "'(*, segment_count: Annotated[int, Ge(ge=0)], stored_bytes: Annotated[int, Ge(ge=0)], authority_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteCompletionAuthority",
  "unit": "export"
}
```

</details>
