# gogurt_listener_runtime.open_private_text_append

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-open-private-text-append:7b9bdec5ab -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fafa0d530d"></a>
- <a id="s-c4d60b7c25"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-cec2a4f4fb"></a>`module`: `gogurt_listener_runtime`
- <a id="s-316c8584eb"></a>`name`: `open_private_text_append`
- <a id="s-0c8385f394"></a>`unit`: `export`

### Declared structure

- <a id="s-5e2fd0d2f4"></a>`kind`: `"function"`
- <a id="s-644e576f5a"></a>`signature`: `"\"(path: 'Path', *, encoding: 'str', errors: 'str \| None') -> 'TextIO'\""`

## Governing policies

- <a id="pa-80791b7534"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.open_private_text_append`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d40897642ce6abb46d63c5679a55344ee4b1fb95f063563f6f4dc6bcf2886bab -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path', *, encoding: 'str', errors: 'str | None') -> 'TextIO'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "open_private_text_append",
  "unit": "export"
}
```

</details>
