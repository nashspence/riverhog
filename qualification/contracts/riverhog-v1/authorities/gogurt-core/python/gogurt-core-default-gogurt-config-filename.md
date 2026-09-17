# gogurt_core.DEFAULT_GOGURT_CONFIG_FILENAME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-default-gogurt-config-filename:06b390954e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df9b116046"></a>
- <a id="s-f638b57d2a"></a>`distribution`: `gogurt-core`
- <a id="s-0f905508ed"></a>`module`: `gogurt_core`
- <a id="s-c348df5fc6"></a>`name`: `DEFAULT_GOGURT_CONFIG_FILENAME`
- <a id="s-80bb45b234"></a>`unit`: `export`

### Declared structure

- <a id="s-7ab14eef8d"></a>`kind`: `"constant"`
- <a id="s-f90b73ad84"></a>`value`: `"gogurt-routes.yaml"`

## Governing policies

- <a id="pa-e1798a67a3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.DEFAULT_GOGURT_CONFIG_FILENAME`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 302fef84eb1e4210f113a2f7c67bb83253fe780dda8ed952cdfeb7cac6effb2b -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-routes.yaml"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "DEFAULT_GOGURT_CONFIG_FILENAME",
  "unit": "export"
}
```

</details>
