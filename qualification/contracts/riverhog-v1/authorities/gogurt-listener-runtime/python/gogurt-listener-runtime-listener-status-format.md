# gogurt_listener_runtime.LISTENER_STATUS_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-status-format:7d149474b0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9a14dafc1"></a>
- <a id="s-bb5ce59ac8"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-973894025d"></a>`module`: `gogurt_listener_runtime`
- <a id="s-f552f8a6c6"></a>`name`: `LISTENER_STATUS_FORMAT`
- <a id="s-586e3ea630"></a>`unit`: `export`

### Declared structure

- <a id="s-ae33847040"></a>`kind`: `"constant"`
- <a id="s-51fd36507b"></a>`value`: `"gogurt-listener-status/v1"`

## Governing policies

- <a id="pa-b7e22060d7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_STATUS_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21fdf6a2a5f5ed92d2abed829432e0c702b43b01651253a857906bfcd6ead50f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-status/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_STATUS_FORMAT",
  "unit": "export"
}
```

</details>
