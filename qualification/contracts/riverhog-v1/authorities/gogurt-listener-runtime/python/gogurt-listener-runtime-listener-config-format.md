# gogurt_listener_runtime.LISTENER_CONFIG_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-config-format:a619f7faef -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6373ed61c"></a>
- <a id="s-d77171ed6d"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-135493b88d"></a>`module`: `gogurt_listener_runtime`
- <a id="s-0de042aad5"></a>`name`: `LISTENER_CONFIG_FORMAT`
- <a id="s-6fb9ee561b"></a>`unit`: `export`

### Declared structure

- <a id="s-5f3cf985f5"></a>`kind`: `"constant"`
- <a id="s-99011fa948"></a>`value`: `"gogurt-listener-config/v1"`

## Governing policies

- <a id="pa-75d4409290"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_CONFIG_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d59fd34d5d3c8d75c2009740f024ae961d27436365a96aaf3b6323ea0fc4c1b5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-config/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_CONFIG_FORMAT",
  "unit": "export"
}
```

</details>
