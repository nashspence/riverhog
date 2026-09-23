# gogurt_listener_runtime.LISTENER_STATE_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-state-schema:24e5a431c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3402b7c4ed"></a>
- <a id="s-80ea7f5377"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-e7d6466030"></a>`module`: `gogurt_listener_runtime`
- <a id="s-f5a6c40636"></a>`name`: `LISTENER_STATE_SCHEMA`
- <a id="s-3044a2d02c"></a>`unit`: `export`

### Declared structure

- <a id="s-40385c2629"></a>`kind`: `"constant"`
- <a id="s-0de26cd6d5"></a>`value`: `1`

## Governing policies

- <a id="pa-571c8278bb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_STATE_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e618ce6616c59db3c38325b6fdef0708e78ac3245bd94e491483ff5d4d2511a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 1
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_STATE_SCHEMA",
  "unit": "export"
}
```

</details>
