# gogurt_core.MIN_GOGURT_INTERVAL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-min-gogurt-interval-seconds:209a3de6af -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8156a8be49"></a>
- <a id="s-52c194f0df"></a>`distribution`: `gogurt-core`
- <a id="s-622c180cc9"></a>`module`: `gogurt_core`
- <a id="s-ccbea54025"></a>`name`: `MIN_GOGURT_INTERVAL_SECONDS`
- <a id="s-ff88e48ae0"></a>`unit`: `export`

### Declared structure

- <a id="s-b03bcd7752"></a>`kind`: `"constant"`
- <a id="s-6d1199ae78"></a>`value`: `0.1`

## Governing policies

- <a id="pa-55e91fde7c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.MIN_GOGURT_INTERVAL_SECONDS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d217ae709c071a701fbd23784d13e2ecc98dcb0a0216ee0adfd260d12e785d58 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 0.1
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "MIN_GOGURT_INTERVAL_SECONDS",
  "unit": "export"
}
```

</details>
