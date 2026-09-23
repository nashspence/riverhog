# a_gogurt_macos_listener.default_listener_paths

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-default-listener-paths:36ddb3894e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-446158cf54"></a>
- <a id="s-f78327dc83"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-6e8c98d993"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-22ea118e25"></a>`name`: `default_listener_paths`
- <a id="s-275ae763a0"></a>`unit`: `export`

### Declared structure

- <a id="s-80cdd73ec2"></a>`kind`: `"function"`
- <a id="s-50edc6ac0c"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerRuntimePaths'\""`

## Governing policies

- <a id="pa-be572e629e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.default_listener_paths`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: abc75314cbf3e9b4e91d85f68d64c29812432a8736850ff2406a5796a7f3d602 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "default_listener_paths",
  "unit": "export"
}
```

</details>
