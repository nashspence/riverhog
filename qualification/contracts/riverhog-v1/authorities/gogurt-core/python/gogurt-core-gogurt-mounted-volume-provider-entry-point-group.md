# gogurt_core.GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-mounted-volume-provide-7865566d62:60e5164907 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ecb0e9eeb"></a>
- <a id="s-0d624ce9b8"></a>`distribution`: `gogurt-core`
- <a id="s-e9f580aafc"></a>`module`: `gogurt_core`
- <a id="s-98e05f54d0"></a>`name`: `GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP`
- <a id="s-3d8a310500"></a>`unit`: `export`

### Declared structure

- <a id="s-3d8e1b12b3"></a>`kind`: `"constant"`
- <a id="s-79031bf8d2"></a>`value`: `"gogurt.mounted-volume-providers"`

## Governing policies

- <a id="pa-62f68f01cf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dddccfd3c1a14fe2d1672a7c8c42da0eeec0d97369bda481ca5eb5feb20bb704 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt.mounted-volume-providers"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP",
  "unit": "export"
}
```

</details>
