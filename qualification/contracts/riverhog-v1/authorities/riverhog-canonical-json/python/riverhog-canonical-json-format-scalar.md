# riverhog_canonical_json.format_scalar

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-format-scalar:ad7a869551 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8ea95bbcf6"></a>
- <a id="s-e776263ec0"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-d1a22767fa"></a>`module`: `riverhog_canonical_json`
- <a id="s-27db32bf26"></a>`name`: `format_scalar`
- <a id="s-6b5a8869dd"></a>`unit`: `export`

### Declared structure

- <a id="s-f249c6578f"></a>`kind`: `"function"`
- <a id="s-3a7bcaf752"></a>`signature`: `"\"(domain: 'ScalarDomain', value: 'int') -> 'str'\""`

## Governing policies

- <a id="pa-45ab5671f7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.format_scalar`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbe5f9f134fdea866cf14f53a461e755ab0e05fd66919c541ac1df80adfff81d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(domain: 'ScalarDomain', value: 'int') -> 'str'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "format_scalar",
  "unit": "export"
}
```

</details>
