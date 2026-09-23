# gogurt_core.route_for_gogurt_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-route-for-gogurt-marker:0b3d53c781 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de36c8cf22"></a>
- <a id="s-2ac8fe7703"></a>`distribution`: `gogurt-core`
- <a id="s-e97b7ba87c"></a>`module`: `gogurt_core`
- <a id="s-352693dd4d"></a>`name`: `route_for_gogurt_marker`
- <a id="s-85ce0137f2"></a>`unit`: `export`

### Declared structure

- <a id="s-05f1c7f8b2"></a>`kind`: `"function"`
- <a id="s-649440e997"></a>`signature`: `"\"(config_file: 'PathInput', route_name: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-e07e6688eb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.route_for_gogurt_marker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee130591331f4cab76833317916228caefc608e4489f4fddb7b0636f90231c79 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'PathInput', route_name: 'str') -> 'str'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "route_for_gogurt_marker",
  "unit": "export"
}
```

</details>
