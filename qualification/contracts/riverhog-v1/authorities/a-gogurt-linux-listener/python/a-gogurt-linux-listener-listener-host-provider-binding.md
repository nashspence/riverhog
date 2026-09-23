# a_gogurt_linux_listener.LISTENER_HOST_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-listener-host-pro-c2d5a8a773:babc78d9b6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42f4a2a586"></a>
- <a id="s-8aa44d32e7"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-a5103e709c"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-f010d07166"></a>`name`: `LISTENER_HOST_PROVIDER_BINDING`
- <a id="s-f8f78c6e6a"></a>`unit`: `export`

### Declared structure

- <a id="s-40329e6df0"></a>`kind`: `"object"`
- <a id="s-3370075c43"></a>`type`: `"gogurt_listener_runtime.platform.ListenerHostProviderBinding"`

## Governing policies

- <a id="pa-62ac86ac76"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.LISTENER_HOST_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8a1806ccb75e37f5ec40741fab93972010ca97842520fd3c6c96cb41ae1d9e9 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "LISTENER_HOST_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
