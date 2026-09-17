# gogurt_listener_runtime.PRIVATE_FILE_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-private-file-mode:af5aa22f6b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91e46dda2b"></a>
- <a id="s-dbbb6176f2"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-c5a992946f"></a>`module`: `gogurt_listener_runtime`
- <a id="s-cfc393da0d"></a>`name`: `PRIVATE_FILE_MODE`
- <a id="s-9f504700d0"></a>`unit`: `export`

### Declared structure

- <a id="s-3502d577bd"></a>`kind`: `"constant"`
- <a id="s-acd7c45820"></a>`value`: `384`

## Governing policies

- <a id="pa-1591c21aed"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.PRIVATE_FILE_MODE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91ce1d32a85c78e5b063c583ded4454e03e4d2d85f3312098fb068d254b46eb2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 384
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "PRIVATE_FILE_MODE",
  "unit": "export"
}
```

</details>
