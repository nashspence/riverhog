# a_gogurt_macos_listener.LISTENER_HOST_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-listener-host-pro-840f5bb69c:dd46f65ceb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5733a7910"></a>
- <a id="s-41144efe87"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-764ec3a18a"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-6388614e15"></a>`name`: `LISTENER_HOST_PROVIDER_BINDING`
- <a id="s-fa54b89ea3"></a>`unit`: `export`

### Declared structure

- <a id="s-438f88f81f"></a>`kind`: `"object"`
- <a id="s-52eb03059e"></a>`type`: `"gogurt_listener_runtime.platform.ListenerHostProviderBinding"`

## Governing policies

- <a id="pa-29eb6255ec"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.LISTENER_HOST_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d02c26637a643beb847af5eadd80460d40ba9fede9474d0e367e0b3cbeda696d -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "LISTENER_HOST_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
