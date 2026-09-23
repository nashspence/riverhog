# gogurt_core.default_gogurt_config_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-default-gogurt-config-file:81d465deed -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb7ae320b2"></a>
- <a id="s-338f3ceaee"></a>`distribution`: `gogurt-core`
- <a id="s-a549dc4c3a"></a>`module`: `gogurt_core`
- <a id="s-bcfe820233"></a>`name`: `default_gogurt_config_file`
- <a id="s-0b47c4efac"></a>`unit`: `export`

### Declared structure

- <a id="s-5cedeed23d"></a>`kind`: `"function"`
- <a id="s-4aca6b6dbc"></a>`signature`: `"\"(config_dir: 'PathInput', *, filename: 'str' = 'gogurt-routes.yaml') -> 'Path'\""`

## Governing policies

- <a id="pa-7b8ec3ef36"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.default_gogurt_config_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c9d4c95b5f581b96fdae3f5f648d0a71e29d2dd2dda9344ebed6d275fb84957 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_dir: 'PathInput', *, filename: 'str' = 'gogurt-routes.yaml') -> 'Path'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "default_gogurt_config_file",
  "unit": "export"
}
```

</details>
