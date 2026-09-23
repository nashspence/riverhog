# gogurt_listener_runtime.LISTENER_STATUS_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-status-schema:762d8f60e3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19084b2d57"></a>
- <a id="s-d5ae59c1c3"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-90c71950bb"></a>`module`: `gogurt_listener_runtime`
- <a id="s-922a7810f0"></a>`name`: `LISTENER_STATUS_SCHEMA`
- <a id="s-ff7d933c39"></a>`unit`: `export`

### Declared structure

- <a id="s-b270d0a71e"></a>`kind`: `"constant"`
- <a id="s-c4b1478698"></a>`value`: `"gogurt-listener-status/v1"`

## Governing policies

- <a id="pa-1b0c940c00"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_STATUS_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08ce47f32c38f9aeb902ee49a337150c7093b5a6d79f387afb809a2cf1abf55f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-status/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_STATUS_SCHEMA",
  "unit": "export"
}
```

</details>
