# gogurt_core.MAX_GOGURT_INTERVAL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-max-gogurt-interval-seconds:eb27009b19 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d4e9d80f7b"></a>
- <a id="s-62a52d5ceb"></a>`distribution`: `gogurt-core`
- <a id="s-d830a581d0"></a>`module`: `gogurt_core`
- <a id="s-98e1255885"></a>`name`: `MAX_GOGURT_INTERVAL_SECONDS`
- <a id="s-08244c28b6"></a>`unit`: `export`

### Declared structure

- <a id="s-360321f4b3"></a>`kind`: `"constant"`
- <a id="s-20652394b3"></a>`value`: `3600`

## Governing policies

- <a id="pa-cea624aed8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.MAX_GOGURT_INTERVAL_SECONDS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97da3c1549558599e485026070d205fb6b452cd204997f11bcce7236a07a9a6e -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 3600
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "MAX_GOGURT_INTERVAL_SECONDS",
  "unit": "export"
}
```

</details>
