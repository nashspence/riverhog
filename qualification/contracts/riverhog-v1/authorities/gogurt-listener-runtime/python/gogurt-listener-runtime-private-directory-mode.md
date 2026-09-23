# gogurt_listener_runtime.PRIVATE_DIRECTORY_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-private-directory-mode:7bf12b06f7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-749919e863"></a>
- <a id="s-82f5b75302"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-5d4d332aac"></a>`module`: `gogurt_listener_runtime`
- <a id="s-041832e5a2"></a>`name`: `PRIVATE_DIRECTORY_MODE`
- <a id="s-7cab615dea"></a>`unit`: `export`

### Declared structure

- <a id="s-0083419323"></a>`kind`: `"constant"`
- <a id="s-3a72f7389e"></a>`value`: `448`

## Governing policies

- <a id="pa-ee825ae37f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.PRIVATE_DIRECTORY_MODE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4f678930c5878511ce76d84477c8a2799d264e58afa850efeec022bc0163379 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 448
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "PRIVATE_DIRECTORY_MODE",
  "unit": "export"
}
```

</details>
