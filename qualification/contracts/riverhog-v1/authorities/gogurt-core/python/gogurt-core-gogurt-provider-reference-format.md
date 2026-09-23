# gogurt_core.GOGURT_PROVIDER_REFERENCE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-provider-reference-format:defd73b98f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7f28e0f4e"></a>
- <a id="s-8b9b46d5b6"></a>`distribution`: `gogurt-core`
- <a id="s-461aeea837"></a>`module`: `gogurt_core`
- <a id="s-df5702ef7d"></a>`name`: `GOGURT_PROVIDER_REFERENCE_FORMAT`
- <a id="s-ab14ee8885"></a>`unit`: `export`

### Declared structure

- <a id="s-0c3140f9d7"></a>`kind`: `"constant"`
- <a id="s-526d83103e"></a>`value`: `"gogurt-provider-some-implementations/v1"`

## Governing policies

- <a id="pa-afd89a8566"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_PROVIDER_REFERENCE_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a6ee5a55d7bab6ed5eaed4885e10233e5851813254686eb2103a86c513dc7aa9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-provider-some-implementations/v1"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_PROVIDER_REFERENCE_FORMAT",
  "unit": "export"
}
```

</details>
