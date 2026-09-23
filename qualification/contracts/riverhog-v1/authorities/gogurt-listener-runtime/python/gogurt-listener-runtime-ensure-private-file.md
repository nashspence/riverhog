# gogurt_listener_runtime.ensure_private_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-ensure-private-file:14abb1fd86 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-13963b095e"></a>
- <a id="s-b140720124"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-2ec2e85253"></a>`module`: `gogurt_listener_runtime`
- <a id="s-9af8cbf64e"></a>`name`: `ensure_private_file`
- <a id="s-58d8d524c6"></a>`unit`: `export`

### Declared structure

- <a id="s-73a60105ae"></a>`kind`: `"function"`
- <a id="s-4d312a0162"></a>`signature`: `"\"(path: 'Path') -> 'None'\""`

## Governing policies

- <a id="pa-28049ff9db"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ensure_private_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b5558e78187feeb0856bcf47822abe41dfeaacc6c191700fa73d20cbe7c6b28 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ensure_private_file",
  "unit": "export"
}
```

</details>
