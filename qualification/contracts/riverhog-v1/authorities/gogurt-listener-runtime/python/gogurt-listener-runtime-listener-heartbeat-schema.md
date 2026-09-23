# gogurt_listener_runtime.LISTENER_HEARTBEAT_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-heartbeat-schema:632af222e7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b35d953ce"></a>
- <a id="s-ec295d4fbb"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-8958bb9a99"></a>`module`: `gogurt_listener_runtime`
- <a id="s-088109d6ac"></a>`name`: `LISTENER_HEARTBEAT_SCHEMA`
- <a id="s-b0077053db"></a>`unit`: `export`

### Declared structure

- <a id="s-afacb83057"></a>`kind`: `"constant"`
- <a id="s-4f9c1dc6d2"></a>`value`: `"gogurt-listener-heartbeat/v1"`

## Governing policies

- <a id="pa-06210b5cf5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_HEARTBEAT_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4489207ebf95de34d667240eee4848a8c66ec19c86ef66c14230170b0cf3e88 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-heartbeat/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_HEARTBEAT_SCHEMA",
  "unit": "export"
}
```

</details>
