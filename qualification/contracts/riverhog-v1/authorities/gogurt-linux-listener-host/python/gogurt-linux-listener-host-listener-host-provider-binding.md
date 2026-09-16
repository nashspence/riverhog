# gogurt_linux_listener_host.LISTENER_HOST_PROVIDER_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-listener-host-d29705b865:f693735c60 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-be12f965dd"></a>
- <a id="s-8cd158e70e"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-02fffada7e"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-4fb1302c61"></a>`name`: `LISTENER_HOST_PROVIDER_BINDING`
- <a id="s-abd700e6dd"></a>`unit`: `export`

### Declared structure

- <a id="s-ee1111bba0"></a>`kind`: `"object"`
- <a id="s-9ae99620b1"></a>`type`: `"gogurt_listener_runtime.platform.ListenerHostProviderBinding"`

## Governing policies

- <a id="pa-dfe4171228"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.LISTENER_HOST_PROVIDER_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 426ac54e7aa9c315f2e5c1c4978f5dd28ed60ce677dd673efad0bf636eb4d1d1 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "gogurt_listener_runtime.platform.ListenerHostProviderBinding"
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "LISTENER_HOST_PROVIDER_BINDING",
  "unit": "export"
}
```

</details>
