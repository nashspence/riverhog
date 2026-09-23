# gogurt_core.GogurtProviderKind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurtproviderkind:30cca42de5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ea53d70aa"></a>
- <a id="s-22f74b97e3"></a>`distribution`: `gogurt-core`
- <a id="s-9a817f8a54"></a>`module`: `gogurt_core`
- <a id="s-085810ff9f"></a>`name`: `GogurtProviderKind`
- <a id="s-237ae06389"></a>`unit`: `export`

### Declared structure

- <a id="s-6ce3fd4f89"></a>`kind`: `"type-alias"`
- <a id="s-4d4a6d0362"></a>`value`: `"typing.Literal['mounted-volume', 'listener-host']"`

## Governing policies

- <a id="pa-b67a411059"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.GogurtProviderKind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ae5aae0654a7172b99794fa0cd01acb18487f647227ac0b70f680f3d3b6b4e3 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['mounted-volume', 'listener-host']"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GogurtProviderKind",
  "unit": "export"
}
```

</details>
