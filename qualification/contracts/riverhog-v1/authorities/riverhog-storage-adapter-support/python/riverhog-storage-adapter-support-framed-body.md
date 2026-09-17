# riverhog_storage_adapter_support.framed_body

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-framed-body:410ed21aea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-645156724a"></a>
- <a id="s-1f94186257"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-788099a89b"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-f081bf8c43"></a>`name`: `framed_body`
- <a id="s-a4681d14f9"></a>`unit`: `export`

### Declared structure

- <a id="s-92fb8ff48d"></a>`kind`: `"function"`
- <a id="s-a7b3bf37f3"></a>`signature`: `"\"(model: 'BaseModel', content: 'BinaryContent') -> 'Iterator[bytes]'\""`

## Governing policies

- <a id="pa-73bd772b96"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.framed_body`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae4310a946087da64eddc6942e4061e3e1f83cf572d95fa1602845553270b1f1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(model: 'BaseModel', content: 'BinaryContent') -> 'Iterator[bytes]'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "framed_body",
  "unit": "export"
}
```

</details>
