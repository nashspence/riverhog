# gogurt_listener_runtime.GOGURT_LISTENER_HOST_PROVIDER_BINDING_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-gogurt-listener-h-50cd7c5c1a:8c13b17c68 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ebf5392676"></a>
- <a id="s-02955e514c"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-16656de190"></a>`module`: `gogurt_listener_runtime`
- <a id="s-9f0ab4bda1"></a>`name`: `GOGURT_LISTENER_HOST_PROVIDER_BINDING_FORMAT`
- <a id="s-4fc4a03221"></a>`unit`: `export`

### Declared structure

- <a id="s-374a833d7c"></a>`kind`: `"constant"`
- <a id="s-ab68bfcac8"></a>`value`: `"gogurt-listener-host-provider-binding/v1"`

## Governing policies

- <a id="pa-2da39d1ca8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.GOGURT_LISTENER_HOST_PROVIDER_BINDING_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6dec5f6734cb4f92df71e82a7a9f39b8338ce6ca74953dcb544cf7df485a2c4 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-host-provider-binding/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "GOGURT_LISTENER_HOST_PROVIDER_BINDING_FORMAT",
  "unit": "export"
}
```

</details>
