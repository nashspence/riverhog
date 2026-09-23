# a_gogurt_windows_listener.LISTENER_HOST_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-listener-host-p-b7127eadcf:bce14f9b69 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df0b963b85"></a>
- <a id="s-6364449ddd"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-bef3555e3f"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-a46023dd69"></a>`name`: `LISTENER_HOST_PROVIDER_BINDING`
- <a id="s-5de899275f"></a>`unit`: `export`

### Declared structure

- <a id="s-c9f2f43418"></a>`kind`: `"object"`
- <a id="s-4d616b52d8"></a>`type`: `"gogurt_listener_runtime.platform.ListenerHostProviderBinding"`

## Governing policies

- <a id="pa-b04677c7f5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.LISTENER_HOST_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7769df90c09ab2ebe3400285a3e143a7f57d9e2e90dc08bb2e3bb41b9c8ec71 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "LISTENER_HOST_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
