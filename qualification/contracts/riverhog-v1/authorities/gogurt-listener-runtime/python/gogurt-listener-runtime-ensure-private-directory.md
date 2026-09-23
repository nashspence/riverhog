# gogurt_listener_runtime.ensure_private_directory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-ensure-private-directory:c50ec05d45 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe4125ac8a"></a>
- <a id="s-2c6b121d6a"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-e065d5e7a1"></a>`module`: `gogurt_listener_runtime`
- <a id="s-ccbc60b58e"></a>`name`: `ensure_private_directory`
- <a id="s-bd59c3227d"></a>`unit`: `export`

### Declared structure

- <a id="s-d614a0358f"></a>`kind`: `"function"`
- <a id="s-c409400dd4"></a>`signature`: `"\"(path: 'Path') -> 'None'\""`

## Governing policies

- <a id="pa-46eed18881"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ensure_private_directory`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9897d4727d4d0519c0e36618a733c8ea410bbe101202610951dadf07da524c10 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ensure_private_directory",
  "unit": "export"
}
```

</details>
